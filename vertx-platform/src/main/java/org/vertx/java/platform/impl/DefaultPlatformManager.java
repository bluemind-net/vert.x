/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package org.vertx.java.platform.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.impl.DefaultEventBus;
import org.vertx.java.core.file.impl.ClasspathPathResolver;
import org.vertx.java.core.file.impl.ModuleFileSystemPathResolver;
import org.vertx.java.core.impl.CountingCompletionHandler;
import org.vertx.java.core.impl.DefaultContext;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.impl.OrderedExecutorFactory;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.core.net.impl.ServerID;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.platform.PlatformManagerException;
import org.vertx.java.platform.Verticle;
import org.vertx.java.platform.VerticleConstructor;
import org.vertx.java.platform.VerticleFactory;
import org.vertx.java.platform.impl.resolver.BintrayRepoResolver;
import org.vertx.java.platform.impl.resolver.MavenLocalRepoResolver;
import org.vertx.java.platform.impl.resolver.MavenRepoResolver;
import org.vertx.java.platform.impl.resolver.OldRepoResolver;
import org.vertx.java.platform.impl.resolver.RepoResolver;

/**
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 *
 */
public class DefaultPlatformManager implements PlatformManagerInternal {

	private static final Logger log = LoggerFactory.getLogger(DefaultPlatformManager.class);
	private static final int BUFFER_SIZE = 4096;
	private static final String MODS_DIR_PROP_NAME = "vertx.mods";
	private static final String REPOS_FILE_NAME = "repos.txt";
	private static final String DEFAULT_REPOS_FILE_NAME = "default-repos.txt";
	private static final String LOCAL_MODS_DIR = "mods";
	private static final String SYS_MODS_DIR = "sys-mods";
	private static final String VERTX_HOME_SYS_PROP = "vertx.home";
	private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
	private static final String FILE_SEP = System.getProperty("file.separator");
	private static final String CLASSPATH_FILE = "vertx_classpath.txt";
	private static final String SERIALISE_BLOCKING_PROP_NAME = "vertx.serialiseBlockingActions";
	private static final String MODULE_LINK_FILE = "module.link";
	private static final String MOD_JSON_FILE = "mod.json";

	private final VertxInternal vertx;
	// deployment name --> deployment
	protected final Map<String, Deployment> deployments = new ConcurrentHashMap<>();
	// The user mods dir
	private File modRoot;
	private File systemModRoot;
	private String vertxHomeDir;
	private final ConcurrentMap<String, ModuleReference> moduleRefs = new ConcurrentHashMap<>();
	private final Map<String, LanguageImplInfo> languageImpls = new ConcurrentHashMap<>();
	private final Map<String, String> extensionMappings = new ConcurrentHashMap<>();
	private final List<RepoResolver> repos = new ArrayList<>();
	private Handler<Void> exitHandler;
	private ClassLoader platformClassLoader;
	private boolean disableMavenLocal;
	protected final ClusterManager clusterManager;
	protected HAManager haManager;
	private boolean stopped;
	private final Queue<String> tempDeployments = new ConcurrentLinkedQueue<>();
	private Executor backgroundExec;

	protected DefaultPlatformManager() {
		DefaultVertx v = new DefaultVertx();
		this.vertx = new WrappedVertx(v);
		this.clusterManager = v.clusterManager();
		init();
	}

	protected DefaultPlatformManager(int port, String hostname) {
		this(port, hostname, 0, null, false);
	}

	protected DefaultPlatformManager(int port, String hostname, int quorumSize, String haGroup) {
		this(port, hostname, quorumSize, haGroup, true);
	}

	protected DefaultPlatformManager(int port, String hostname, int quorumSize, String haGroup, boolean haEnabled) {
		this.vertx = createVertxSynchronously(port, hostname);
		this.clusterManager = vertx.clusterManager();
		init();
		final DefaultEventBus eb = (DefaultEventBus) vertx.eventBus();
		this.haManager = new HAManager(vertx, eb.serverID(), this, clusterManager, quorumSize, haGroup, haEnabled);
		haManager.setRemoveSubsHandler(new FailoverCompleteHandler() {
			@Override
			public void handle(String nodeID, JsonObject haInfo, boolean failed) {
				JsonObject jsid = haInfo.getObject("server_id");
				if (jsid != null) {
					ServerID sid = new ServerID(jsid.getInteger("port"), jsid.getString("host"));
					eb.cleanSubsForServerID(sid);
				}
			}
		});
	}

	private VertxInternal createVertxSynchronously(int port, String hostname) {
		final AtomicReference<AsyncResult<Vertx>> resultReference = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		DefaultVertx v = new DefaultVertx(port, hostname, new Handler<AsyncResult<Vertx>>() {
			@Override
			public void handle(AsyncResult<Vertx> result) {
				resultReference.set(result);
				latch.countDown();
			}
		});
		try {
			if (!latch.await(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Unable to start Vertx within 10 seconds");
			}
			AsyncResult<Vertx> result = resultReference.get();
			if (result.failed()) {
				throw new IllegalStateException("Unable to start Vertx", result.cause());
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
		return new WrappedVertx(v);
	}

	private void init() {
		ClassLoader tccl = Thread.currentThread().getContextClassLoader();
		this.platformClassLoader = tccl != null ? tccl : getClass().getClassLoader();

		if (System.getProperty(SERIALISE_BLOCKING_PROP_NAME, "false").equalsIgnoreCase("true")) {
			this.backgroundExec = new OrderedExecutorFactory(vertx.getBackgroundPool()).getExecutor();
		} else {
			this.backgroundExec = vertx.getBackgroundPool();
		}

		String modDir = System.getProperty(MODS_DIR_PROP_NAME);
		if (modDir != null && !modDir.trim().equals("")) {
			modRoot = new File(modDir);
		} else {
			// Default to local module directory
			modRoot = new File(LOCAL_MODS_DIR);
		}
		vertxHomeDir = System.getProperty(VERTX_HOME_SYS_PROP);
		if (vertxHomeDir == null || modDir != null) {
			systemModRoot = modRoot;
		} else {
			systemModRoot = new File(vertxHomeDir, SYS_MODS_DIR);
		}
		// If running on CI we don't want to use maven local to get any modules - this
		// is because they can
		// get stale easily - we must always get them from external repos
		this.disableMavenLocal = System.getenv("VERTX_DISABLE_MAVENLOCAL") != null;
		loadLanguageMappings();
		loadRepos();
	}

	@Override
	public void registerExitHandler(Handler<Void> handler) {
		this.exitHandler = handler;
	}

	@Override
	public void deployVerticle(VerticleConstructor main, JsonObject config, URL[] classpath, int instances,
			String includes, Handler<AsyncResult<String>> doneHandler) {
		deployVerticle(false, false, main, config, classpath, instances, includes, doneHandler);
	}

	@Override
	public void deployWorkerVerticle(boolean multiThreaded, VerticleConstructor main, JsonObject config,
			URL[] classpath, int instances, String includes, Handler<AsyncResult<String>> doneHandler) {
		deployVerticle(true, multiThreaded, main, config, classpath, instances, includes, doneHandler);
	}

	@Override
	public synchronized void undeploy(final String deploymentID, final Handler<AsyncResult<Void>> doneHandler) {
		runInBackground(new Runnable() {
			public void run() {
				if (deploymentID == null) {
					throw new NullPointerException("deploymentID cannot be null");
				}
				final Deployment dep = deployments.get(deploymentID);
				if (dep == null) {
					throw new PlatformManagerException("There is no deployment with id " + deploymentID);
				}
				doUndeploy(dep, doneHandler);
			}
		}, wrapDoneHandler(doneHandler));
	}

	@Override
	public synchronized void undeployAll(final Handler<AsyncResult<Void>> doneHandler) {
		List<String> parents = new ArrayList<>();
		for (Map.Entry<String, Deployment> entry : deployments.entrySet()) {
			if (entry.getValue().parentDeploymentName == null) {
				parents.add(entry.getKey());
			}
		}

		final CountingCompletionHandler<Void> count = new CountingCompletionHandler<>(vertx, parents.size());
		count.setHandler(doneHandler);

		for (String name : parents) {
			undeploy(name, new Handler<AsyncResult<Void>>() {
				public void handle(AsyncResult<Void> res) {
					if (res.failed()) {
						count.failed(res.cause());
					} else {
						count.complete();
					}
				}
			});
		}
	}

	@Override
	public Map<String, Integer> listInstances() {
		Map<String, Integer> map = new HashMap<>();
		for (Map.Entry<String, Deployment> entry : deployments.entrySet()) {
			map.put(entry.getKey(), entry.getValue().verticles.size());
		}
		return map;
	}

	@Override
	public void pullInDependencies(final String moduleName, final Handler<AsyncResult<Void>> doneHandler) {
		final Handler<AsyncResult<Void>> wrapped = wrapDoneHandler(doneHandler);
		runInBackground(new Runnable() {
			public void run() {
				ModuleIdentifier modID = new ModuleIdentifier(moduleName); // Validates it
				doPullInDependencies(modRoot, modID);
				doneHandler.handle(new DefaultFutureResult<>((Void) null));
			}
		}, wrapped);
	}

	@Override
	public void makeFatJar(final String moduleName, final String directory,
			final Handler<AsyncResult<Void>> doneHandler) {
		final Handler<AsyncResult<Void>> wrapped = wrapDoneHandler(doneHandler);
		runInBackground(new Runnable() {
			public void run() {
				ModuleIdentifier modID = new ModuleIdentifier(moduleName); // Validates it
				doMakeFatJar(modRoot, modID, directory);
				doneHandler.handle(new DefaultFutureResult<>((Void) null));
			}
		}, wrapped);
	}

	@Override
	public Vertx vertx() {
		return this.vertx;
	}

	@Override
	public void exit() {
		// We tell the cluster manager to leave - this is because Hazelcast uses non
		// daemon threads which will prevent
		// JVM exit and shutdown hooks to be called
		ClusterManager mgr = vertx.clusterManager();
		if (mgr != null) {
			vertx.clusterManager().leave();
		}
		if (exitHandler != null) {
			exitHandler.handle(null);
		}
	}

	@Override
	public JsonObject config() {
		VerticleHolder holder = getVerticleHolder();
		return holder == null ? null : holder.config;
	}

	@Override
	public Logger logger() {
		VerticleHolder holder = getVerticleHolder();
		return holder == null ? null : holder.logger;
	}

	@Override
	public Map<String, Deployment> deployments() {
		return new HashMap<>(deployments);
	}

	private <T> void runInBackground(final Runnable runnable, final Handler<AsyncResult<T>> doneHandler) {
		final DefaultContext context = vertx.getOrCreateContext();
		backgroundExec.execute(new Runnable() {
			public void run() {
				try {
					vertx.setContext(context);
					runnable.run();
				} catch (Throwable t) {
					if (doneHandler != null) {
						doneHandler.handle(new DefaultFutureResult<T>(t));
					} else {
						log.error("Failed to run task", t);
					}
				} finally {
					vertx.setContext(null);
				}
			}
		});
	}

	private void deployVerticle(final boolean worker, final boolean multiThreaded, final VerticleConstructor main,
			final JsonObject config, URL[] classpath, final int instances, final String includes,
			final Handler<AsyncResult<String>> doneHandler) {
		final File currentModDir = getDeploymentModDir();
		final URL[] cp;
		if (classpath == null) {
			// Use the current module's/verticle's classpath
			cp = getClasspath();
			if (cp == null) {
				throw new IllegalStateException(
						"Cannot find parent classpath. Perhaps you are deploying the verticle from a non Vert.x thread?");
			}
		} else {
			cp = classpath;
		}
		final Handler<AsyncResult<String>> wrapped = wrapDoneHandler(doneHandler);
		runInBackground(new Runnable() {
			public void run() {
				doDeployVerticle(worker, multiThreaded, main, config, cp, instances, currentModDir, includes, wrapped);
			}
		}, wrapped);
	}

	private String getDeploymentName() {
		VerticleHolder holder = getVerticleHolder();
		return holder == null ? null : holder.deployment.name;
	}

	private URL[] getClasspath() {
		VerticleHolder holder = getVerticleHolder();
		return holder == null ? null : holder.deployment.classpath;
	}

	private File getDeploymentModDir() {
		VerticleHolder holder = getVerticleHolder();
		return holder == null ? null : holder.deployment.modDir;
	}

	private void doPullInDependencies(File modRoot, ModuleIdentifier modID) {
		File modDir = new File(modRoot, modID.toString());
		if (!modDir.exists()) {
			throw new PlatformManagerException("Cannot find module");
		}
		JsonObject conf = loadModuleConfig(createModJSONFile(modDir), modID);
		if (conf == null) {
			throw new PlatformManagerException("Module " + modID + " does not contain a mod.json");
		}
		ModuleFields fields = new ModuleFields(conf);
		List<String> mods = new ArrayList<>();
		String includes = fields.getIncludes();
		if (includes != null) {
			mods.addAll(Arrays.asList(parseIncludeString(includes)));
		}
		String deploys = fields.getDeploys();
		if (deploys != null) {
			mods.addAll(Arrays.asList(parseIncludeString(deploys)));
		}
		if (!mods.isEmpty()) {
			File internalModsDir = new File(modDir, "mods");
			if (!internalModsDir.exists()) {
				if (!internalModsDir.mkdir()) {
					throw new PlatformManagerException("Failed to create directory " + internalModsDir);
				}
			}
			for (String modName : mods) {
				File internalModDir = new File(internalModsDir, modName);
				if (!internalModDir.exists()) {
					ModuleIdentifier theModID = new ModuleIdentifier(modName);
					ModuleZipInfo zipInfo = getModule(theModID);
					if (zipInfo.filename != null) {
						if (!internalModDir.mkdir()) {
							throw new PlatformManagerException("Failed to create directory " + internalModDir);
						}
						unzipModuleData(internalModDir, zipInfo, true);
						log.info("Module " + modName + " successfully installed in mods dir of " + modName);
						// Now recurse so we bring in all of the deps
						doPullInDependencies(internalModsDir, theModID);
					}
				}
			}
		}
	}

	private void doMakeFatJar(File modRoot, ModuleIdentifier modID, String directory) {

	}

	void addDirToZip(File topDir, File dir, ZipOutputStream out) throws Exception {

		Path top = Paths.get(topDir.getAbsolutePath());

		File[] files = dir.listFiles();
		byte[] buffer = new byte[4096];

		for (int i = 0; i < files.length; i++) {
			Path entry = Paths.get(files[i].getAbsolutePath());
			Path rel = top.relativize(entry);
			String entryName = rel.toString();
			if (files[i].isDirectory()) {
				entryName += FILE_SEP;
			}

			if (!files[i].isDirectory()) {
				out.putNextEntry(new ZipEntry(entryName.replace('\\', '/')));
				try (FileInputStream in = new FileInputStream(files[i])) {
					int bytesRead;
					while ((bytesRead = in.read(buffer)) != -1) {
						out.write(buffer, 0, bytesRead);
					}
				}
				out.closeEntry();
			}

			if (files[i].isDirectory()) {
				addDirToZip(topDir, files[i], out);
			}
		}
	}

	// This makes sure the result is handled on the calling context
	private <T> Handler<AsyncResult<T>> wrapDoneHandler(final Handler<AsyncResult<T>> doneHandler) {
		if (doneHandler == null) {
			// Just create one which logs out any failure, otherwise we will have silent
			// failures when no handler
			// is specified
			return new AsyncResultHandler<T>() {
				@Override
				public void handle(AsyncResult<T> res) {
					if (res.failed()) {
						vertx.reportException(res.cause());
					}
				}
			};
		} else {
			final DefaultContext context = vertx.getContext();
			return new AsyncResultHandler<T>() {
				@Override
				public void handle(final AsyncResult<T> res) {
					if (context == null) {
						doneHandler.handle(res);
					} else {
						context.execute(new Runnable() {
							public void run() {
								doneHandler.handle(res);
							}
						});
					}
				}
			};
		}
	}

	// Recurse up through the parent deployments and return the the module name for
	// the first one
	// which has one, or if there is no enclosing module just use the deployment
	// name
	private ModuleIdentifier getEnclosingModID() {
		VerticleHolder holder = getVerticleHolder();
		Deployment dep = holder == null ? null : holder.deployment;
		while (dep != null) {
			if (dep.modID != null) {
				return dep.modID;
			} else {
				String parentDepName = dep.parentDeploymentName;
				if (parentDepName != null) {
					dep = deployments.get(parentDepName);
				} else {
					// Top level - deployed as verticle not module
					// Just use the deployment name
					return ModuleIdentifier.createInternalModIDForVerticle(dep.name);
				}
			}
		}
		return null; // We are at the top level already
	}

	private ModuleReference getModuleReference(String moduleKey, URL[] urls, boolean loadFromModuleFirst) {
		ModuleReference mr = moduleRefs.get(moduleKey);
		if (mr == null) {
			mr = new ModuleReference(this, moduleKey,
					new ModuleClassLoader(moduleKey, platformClassLoader, urls, loadFromModuleFirst), false);
			ModuleReference prev = moduleRefs.putIfAbsent(moduleKey, mr);
			if (prev != null) {
				mr = prev;
			}
		}
		return mr;
	}

	private void doDeployVerticle(boolean worker, boolean multiThreaded, final VerticleConstructor main,
			final JsonObject config, final URL[] urls, int instances, File currentModDir, String includes,
			Handler<AsyncResult<String>> doneHandler) {
		checkWorkerContext();

		if (main == null) {
			throw new NullPointerException("main cannot be null");
		}
		if (urls == null) {
			throw new IllegalStateException("deployment classpath for deploy is null");
		}

		// There is one module class loader per enclosing module + the name of the
		// verticle.
		// If there is no enclosing module, there is one per top level verticle
		// deployment
		// E.g. if a module A deploys "foo.js" as a verticle then all instances of
		// foo.js deployed by the enclosing
		// module will share a module class loader
		String depName = genDepName();
		ModuleIdentifier enclosingModName = getEnclosingModID();
		String moduleKey;
		boolean loadFromModuleFirst;
		if (enclosingModName == null) {
			// We are at the top level - just use the deployment name as the key
			moduleKey = ModuleIdentifier.createInternalModIDForVerticle(depName).toString();
			loadFromModuleFirst = false;
		} else {
			// Use the enclosing module name / or enclosing verticle PLUS the main
			moduleKey = enclosingModName.toString() + "#" + main;
			VerticleHolder holder = getVerticleHolder();
			Deployment dep = holder.deployment;
			loadFromModuleFirst = dep.loadFromModuleFirst;
		}

		ModuleReference mr = getModuleReference(moduleKey, urls, loadFromModuleFirst);

		if (enclosingModName != null) {
			// Add the enclosing module as a parent
			ModuleReference parentRef = moduleRefs.get(enclosingModName.toString());

			if (mr.mcl.addReference(parentRef)) {
				parentRef.incRef();
			}
		}

		if (includes != null) {
			loadIncludedModules(modRoot, currentModDir, mr, includes);
		}
		doDeploy(depName, false, worker, multiThreaded, null, main, null, config, urls, null, instances, currentModDir,
				mr, modRoot, false, loadFromModuleFirst, doneHandler);
	}

	private static void checkWorkerContext() {
		Thread t = Thread.currentThread();
		if (!t.getName().startsWith("vert.x-worker-thread")) {
			throw new IllegalStateException("Not a worker thread");
		}
	}

	private void loadLanguageMappings() {
		// The only language that Vert.x understands out of the box is Java, so we add
		// the default runtime and
		// extension mapping for that. This can be overridden in langs.properties
		languageImpls.put("java", new LanguageImplInfo(null, "org.vertx.java.platform.impl.java.JavaVerticleFactory"));
		extensionMappings.put("java", "java");
		extensionMappings.put("class", "java");

	}

	private File locateModule(File modRoot, File currentModDir, ModuleIdentifier modID) {
		if (currentModDir != null) {
			// Nested moduleRefs - look inside current module dir
			File modDir = new File(new File(currentModDir, LOCAL_MODS_DIR), modID.toString());
			if (modDir.exists()) {
				return modDir;
			}
		}
		File modDir = new File(modRoot, modID.toString());
		if (modDir.exists()) {
			return modDir;
		}
		// Try with the static modDir
		modDir = new File(this.modRoot, modID.toString());
		if (modDir.exists()) {
			return modDir;
		}
		// Now try with system mod dir
		if (!systemModRoot.equals(modRoot)) {
			modDir = new File(systemModRoot, modID.toString());
			if (modDir.exists()) {
				return modDir;
			}
		}
		return null;
	}

	private JsonObject loadModJSONFromURL(ModuleIdentifier modID, URL url) {
		try {
			try (Scanner scanner = new Scanner(url.openStream(), "UTF-8").useDelimiter("\\A")) {
				String conf = scanner.next();
				return new JsonObject(conf);
			} catch (NoSuchElementException e) {
				throw new PlatformManagerException("Module " + modID + " contains an empty mod.json file");
			} catch (DecodeException e) {
				throw new PlatformManagerException("Module " + modID + " mod.json contains invalid json");
			}
		} catch (IOException e) {
			return null;
		}
	}

	private static class ModuleInfo {
		final JsonObject modJSON;
		final List<URL> cp;
		final File modDir;

		private ModuleInfo(JsonObject modJSON, List<URL> cp, File modDir) {
			this.modJSON = modJSON;
			this.cp = cp;
			this.modDir = modDir;
		}
	}

	private ModuleInfo loadModuleInfo(File modDir, ModuleIdentifier modID) {

		List<URL> cpList = new ArrayList<>();
		JsonObject modJSON = null;
		File targetModDir = modDir;
		// Look for link file
		File linkFile = new File(modDir, MODULE_LINK_FILE);
		if (linkFile.exists()) {
			// Load the path from the file
			try (Scanner scanner = new Scanner(linkFile, "UTF-8").useDelimiter("\\A")) {
				String path = scanner.next().trim();
				File cpFile = new File(path, CLASSPATH_FILE);
				if (!cpFile.exists()) {
					throw new PlatformManagerException(
							"Module link file: " + linkFile + " points to path without vertx_classpath.txt");
				}
				// Load the cp
				cpList = new ArrayList<>();
				try (Scanner scanner2 = new Scanner(cpFile, "UTF-8")) {
					while (scanner2.hasNextLine()) {
						String entry = scanner2.nextLine().trim();
						if (!entry.startsWith("#") && !entry.equals("")) { // Skip blanks lines and comments
							File fentry = new File(entry);
							if (!fentry.isAbsolute()) {
								fentry = new File(path, entry);
							}
							URL url = fentry.toURI().toURL();
							cpList.add(url);
							if (fentry.exists() && fentry.isDirectory()) {
								File[] files = fentry.listFiles();
								for (File file : files) {
									String fPath = file.getCanonicalPath();
									if (fPath.endsWith(".jar") || fPath.endsWith(".zip")) {
										cpList.add(file.getCanonicalFile().toURI().toURL());
									}
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					throw new PlatformManagerException(e);
				}
				ClassLoader cl = new ModJSONClassLoader(cpList.toArray(new URL[cpList.size()]), platformClassLoader);
				URL url = cl.getResource(MOD_JSON_FILE);
				if (url != null) {
					// Find the file for the url
					Path p = ClasspathPathResolver.urlToPath(url);
					Path parent = p.getParent();
					// And we set the directory where mod.json is as the module directory
					modDir = parent.toFile();
					modJSON = loadModJSONFromURL(modID, url);
				} else {
					throw new PlatformManagerException("Cannot find mod.json");
				}
			} catch (Exception e) {
				throw new PlatformManagerException(e);
			}
		}

		if (modJSON == null) {
			File modJSONFile = createModJSONFile(targetModDir);
			if (modJSONFile.exists()) {
				modJSON = loadModuleConfig(modJSONFile, modID);
			}
		}
		cpList.addAll(getModuleLibClasspath(targetModDir));

		if (modJSON == null) {
			throw new PlatformManagerException(
					"Module directory " + modDir + " contains no mod.json nor module.link file");
		}

		cpList.addAll(getModuleLibClasspath(modDir));
		return new ModuleInfo(modJSON, cpList, modDir);
	}

	private JsonObject loadModuleConfig(File modJSON, ModuleIdentifier modID) {
		// Checked the byte code produced, .close() is called correctly, so the warning
		// can be suppressed
		try (Scanner scanner = new Scanner(modJSON, "UTF-8").useDelimiter("\\A")) {
			String conf = scanner.next();
			return new JsonObject(conf);
		} catch (FileNotFoundException e) {
			throw new PlatformManagerException("Module " + modID + " does not contain a mod.json file");
		} catch (NoSuchElementException e) {
			throw new PlatformManagerException("Module " + modID + " contains an empty mod.json file");
		} catch (DecodeException e) {
			throw new PlatformManagerException("Module " + modID + " mod.json contains invalid json");
		}
	}

	private List<URL> loadIncludedModules(File modRoot, File currentModuleDir, ModuleReference mr,
			String includesString) {
		Set<String> included = new HashSet<>();
		List<URL> includedCP = new ArrayList<>();
		included.add(mr.moduleKey);
		doLoadIncludedModules(modRoot, currentModuleDir, mr, includesString, included, includedCP);
		return includedCP;
	}

	private void doLoadIncludedModules(File modRoot, File currentModuleDir, ModuleReference mr, String includesString,
			Set<String> included, List<URL> includedCP) {
		checkWorkerContext();
		for (String moduleName : parseIncludeString(includesString)) {
			ModuleIdentifier modID = new ModuleIdentifier(moduleName);
			if (included.contains(modID.toString())) {
				log.warn("Module " + modID + " is included more than once in chain of includes");
			} else {
				included.add(modID.toString());
				ModuleReference includedMr = moduleRefs.get(moduleName);
				if (includedMr == null) {
					File modDir = locateModule(modRoot, currentModuleDir, modID);
					if (modDir == null) {
						doInstallMod(modID);
					}
					modDir = locateModule(modRoot, currentModuleDir, modID);
					ModuleInfo info = loadModuleInfo(modDir, modID);
					ModuleFields fields = new ModuleFields(info.modJSON);
					includedCP.addAll(info.cp);

					boolean res = fields.isResident();
					includedMr = new ModuleReference(this, moduleName,
							new ModuleClassLoader(modID.toString(), platformClassLoader,
									info.cp.toArray(new URL[info.cp.size()]), fields.isLoadFromModuleFirst()),
							res);
					ModuleReference prev = moduleRefs.putIfAbsent(moduleName, includedMr);
					if (prev != null) {
						includedMr = prev;
					}
					String includes = fields.getIncludes();
					if (includes != null) {
						doLoadIncludedModules(modRoot, modDir, includedMr, includes, included, includedCP);
					}
				}
				if (mr.mcl.addReference(includedMr)) {
					includedMr.incRef();
				}

			}
		}
	}

	private List<URL> getModuleLibClasspath(File modDir) {
		List<URL> urls = new ArrayList<>();
		// Add the classpath for this module
		try {
			if (modDir.exists()) {
				urls.add(modDir.toURI().toURL());
				File libDir = new File(modDir, "lib");
				if (libDir.exists()) {
					File[] jars = libDir.listFiles();
					for (File jar : jars) {
						URL jarURL = jar.toURI().toURL();
						urls.add(jarURL);
					}
				}
			}
			return urls;
		} catch (MalformedURLException e) {
			// Won't happen
			throw new PlatformManagerException(e);
		}
	}

	private static String[] parseIncludeString(String sincludes) {
		sincludes = sincludes.trim();
		if ("".equals(sincludes)) {
			log.error("Empty include string");
			return null;
		}
		String[] arr = sincludes.split(",");
		if (arr != null) {
			for (int i = 0; i < arr.length; i++) {
				arr[i] = arr[i].trim();
			}
		}
		return arr;
	}

	private InputStream findReposFile() {
		// First we look for repos.txt on the classpath
		InputStream is = platformClassLoader.getResourceAsStream(REPOS_FILE_NAME);
		if (is == null) {
			// Now we look for repos-default.txt which is included in the vertx-platform.jar
			is = platformClassLoader.getResourceAsStream(DEFAULT_REPOS_FILE_NAME);
		}
		return is;
	}

	private void loadRepos() {
		try (InputStream is = findReposFile()) {
			if (is != null) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(is));
				String line;
				while ((line = rdr.readLine()) != null) {
					line = line.trim();
					if (line.isEmpty() || line.startsWith("#")) {
						// blank line or comment
						continue;
					}
					int colonPos = line.indexOf(':');
					if (colonPos == -1 || colonPos == line.length() - 1) {
						throw new IllegalArgumentException("Invalid repo: " + line);
					}
					String type = line.substring(0, colonPos);
					String repoID = line.substring(colonPos + 1);
					RepoResolver resolver;
					switch (type) {
					case "mavenLocal":
						if (disableMavenLocal) {
							continue;
						}
						resolver = new MavenLocalRepoResolver(repoID);
						break;
					case "maven":
						resolver = new MavenRepoResolver(vertx, repoID);
						break;
					case "bintray":
						resolver = new BintrayRepoResolver(vertx, repoID);
						break;
					case "old":
						resolver = new OldRepoResolver(vertx, repoID);
						break;
					default:
						throw new IllegalArgumentException("Unknown repo type: " + type);
					}
					repos.add(resolver);
				}
			}
		} catch (IOException e) {
			log.error("Failed to load repos file ", e);
		}
	}

	private void doInstallMod(final ModuleIdentifier modID) {
		checkWorkerContext();
		if (repos.isEmpty()) {
			throw new PlatformManagerException("No repositories configured!");
		}
		if (locateModule(modRoot, null, modID) != null) {
			throw new PlatformManagerException("Module is already installed");
		}
		ModuleZipInfo info = getModule(modID);
		unzipModule(modID, info, true);
	}

	private ModuleZipInfo getModule(ModuleIdentifier modID) {
		String fileName = generateTmpFileName() + ".zip";
		for (RepoResolver resolver : repos) {
			if (resolver.getModule(fileName, modID)) {
				return new ModuleZipInfo(resolver.isOldStyle(), fileName);
			}
		}
		throw new PlatformManagerException("Module " + modID + " not found in any repositories");
	}

	private static String generateTmpFileName() {
		return TEMP_DIR + FILE_SEP + "vertx-" + UUID.randomUUID().toString();
	}

	static File unzipIntoTmpDir(ModuleZipInfo zipInfo, boolean deleteZip) {
		String tdir = generateTmpFileName();
		File tdest = new File(tdir);
		if (!tdest.mkdir()) {
			throw new PlatformManagerException("Failed to create directory " + tdest);
		}
		unzipModuleData(tdest, zipInfo, deleteZip);
		return tdest;
	}

	private void checkCreateModDirs() {
		checkCreateRoot(modRoot);
		checkCreateRoot(systemModRoot);
	}

	private void checkCreateRoot(File modRoot) {
		if (!modRoot.exists()) {
			String smodRoot;
			try {
				smodRoot = modRoot.getCanonicalPath();
			} catch (IOException e) {
				throw new PlatformManagerException(e);
			}
			vertx.fileSystem().mkdirSync(smodRoot, true);
		}
	}

	private File createModJSONFile(File modDir) {
		return new File(modDir, MOD_JSON_FILE);
	}

	private void unzipModule(final ModuleIdentifier modID, final ModuleZipInfo zipInfo, boolean deleteZip) {
		// We synchronize to prevent a race whereby it tries to unzip the same module at
		// the
		// same time (e.g. deployModule for the same module name has been called in
		// parallel)
		String modName = modID.toString();
		synchronized (modName.intern()) {

			checkCreateModDirs();

			File fdest = new File(modRoot, modName);
			File sdest = new File(systemModRoot, modName);
			if (fdest.exists() || sdest.exists()) {
				// This can happen if the same module is requested to be installed
				// at around the same time
				// It's ok if this happens
				log.warn("Module " + modID + " is already installed");
				return;
			}

			// Unzip into temp dir first
			File tdest = unzipIntoTmpDir(zipInfo, deleteZip);

			// Check if it's a system module
			JsonObject conf = loadModuleConfig(createModJSONFile(tdest), modID);
			ModuleFields fields = new ModuleFields(conf);

			boolean system = fields.isSystem();

			// Now copy it to the proper directory
			String moveFrom = tdest.getAbsolutePath();
			safeMove(moveFrom, system ? sdest.getAbsolutePath() : fdest.getAbsolutePath());
			log.info("Module " + modID + " successfully installed");
		}
	}

	// We actually do a copy and delete since move doesn't always work across
	// volumes
	private void safeMove(String source, String dest) {
		// Try and move first - it's more efficient
		try {
			vertx.fileSystem().moveSync(source, dest);
		} catch (Exception e) {
			// And fall back to copying
			try {
				vertx.fileSystem().copySync(source, dest, true);
				vertx.fileSystem().deleteSync(source, true);
			} catch (Exception e2) {
				throw new PlatformManagerException("Failed to copy module", e2);
			}
		}
	}

	private static String removeTopDir(String entry) {
		int pos = entry.indexOf(FILE_SEP);
		if (pos != -1) {
			entry = entry.substring(pos + 1);
		}
		return entry;
	}

	static private void unzipModuleData(final File directory, final ModuleZipInfo zipinfo, boolean deleteZip) {
		try (InputStream is = new BufferedInputStream(new FileInputStream(zipinfo.filename));
				ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				String entryName = zipinfo.oldStyle ? removeTopDir(entry.getName()) : entry.getName();
				if (!entryName.isEmpty()) {
					File entryFile = new File(directory, entryName);
					if (entry.isDirectory()) {
						if (!entryFile.exists() && !entryFile.mkdirs()) {
							throw new PlatformManagerException("Failed to create directory");
						}
					} else {
						File dir = entryFile.getParentFile();
						if (dir != null && !dir.exists() && !dir.mkdirs()) {
							throw new PlatformManagerException("Failed to create parent directory");
						}
						int count;
						byte[] buff = new byte[BUFFER_SIZE];
						BufferedOutputStream dest = null;
						try {
							OutputStream fos = new FileOutputStream(entryFile);
							dest = new BufferedOutputStream(fos, BUFFER_SIZE);
							while ((count = zis.read(buff, 0, BUFFER_SIZE)) != -1) {
								dest.write(buff, 0, count);
							}
							dest.flush();
						} finally {
							if (dest != null) {
								dest.close();
							}
						}
					}
				}
			}
		} catch (Exception e) {
			throw new PlatformManagerException("Failed to unzip module", e);
		} finally {
			if (deleteZip) {
				if (!new File(zipinfo.filename).delete()) {
					log.error("Failed to delete zip");
				}
			}
		}
	}

	// We calculate a path adjustment that can be used by the fileSystem object
	// so that the *effective* working directory can be the module directory
	// this allows moduleRefs to read and write the file system as if they were
	// in the module dir, even though the actual working directory will be
	// wherever vertx run or vertx runmod was called from
	private void setPathResolver(File modDir) {
		DefaultContext context = vertx.getContext();
		if (modDir != null) {
			// Module deployed from file system or verticle deployed by module deployed from
			// file system
			Path pmodDir = Paths.get(modDir.getAbsolutePath());
			context.setPathResolver(new ModuleFileSystemPathResolver(pmodDir));
		} else {
			// Module deployed from classpath or verticle deployed from module deployed from
			// classpath
			context.setPathResolver(new ClasspathPathResolver());
		}
	}

	private static String genDepName() {
		return "deployment-" + UUID.randomUUID().toString();
	}

	private void doDeploy(final String depID, boolean autoRedeploy, boolean worker, boolean multiThreaded,
			String langMod, VerticleConstructor theMain, final ModuleIdentifier modID, final JsonObject config,
			final URL[] urls, final URL[] includedURLs, int instances, final File modDir, final ModuleReference mr,
			final File modRoot, final boolean ha, final boolean loadFromModuleFirst,
			Handler<AsyncResult<String>> dHandler) {
		checkWorkerContext();

		if (dHandler == null) {
			// Add a simple one that just logs, so deploy failures aren't lost if the user
			// doesn't specify a handler
			dHandler = new Handler<AsyncResult<String>>() {
				@Override
				public void handle(AsyncResult<String> ar) {
					if (ar.failed()) {
						log.error("Failed to deploy", ar.cause());
					}
				}
			};
		}
		final Handler<AsyncResult<String>> doneHandler = dHandler;

		final String deploymentID = depID != null ? depID : genDepName();

		log.debug("Deploying name : " + deploymentID + " main: " + theMain + " instances: " + instances);

		// How we determine which language implementation to use:
		// 1. Look for the optional 'lang-impl' field - this can be used to specify the
		// exact language module to use
		// 2. Look for a prefix on the main, e.g.
		// 'groovy:org.foo.myproject.MyGroovyMain' would force the groovy
		// language impl to be used
		// 3. If there is no prefix, then look at the extension, if any. If there is an
		// extension mapping for that
		// extension, use that.
		// 4. No prefix and no extension mapping - use the default runtime

		LanguageImplInfo langImplInfo = languageImpls.get("java");

		// Include the language impl module as a parent of the classloader
		if (langImplInfo.moduleName != null && !langImplInfo.moduleName.isEmpty()) {
			loadIncludedModules(modRoot, modDir, mr, langImplInfo.moduleName);
		}

		String parentDeploymentName = getDeploymentName();

		if (parentDeploymentName != null) {
			Deployment parentDeployment = deployments.get(parentDeploymentName);
			if (parentDeployment == null) {
				// This means the parent has already been undeployed - we must not deploy the
				// child
				throw new PlatformManagerException("Parent has already been undeployed!");
			}
			parentDeployment.childDeployments.add(deploymentID);
		}

		final VerticleFactory verticleFactory;

		try {
			verticleFactory = mr.getVerticleFactory(langImplInfo.factoryName, vertx, new DefaultContainer(this));
		} catch (Throwable t) {
			throw new PlatformManagerException("Failed to instantiate verticle factory", t);
		}

		final CountingCompletionHandler<Void> aggHandler = new CountingCompletionHandler<>(vertx, instances);
		aggHandler.setHandler(new Handler<AsyncResult<Void>>() {
			@Override
			public void handle(AsyncResult<Void> res) {
				if (res.failed()) {
					doneHandler.handle(new DefaultFutureResult<String>(res.cause()));
				} else {
					doneHandler.handle(new DefaultFutureResult<>(deploymentID));
				}
			}
		});

		final Deployment deployment = new Deployment(deploymentID, theMain, modID, instances,
				config == null ? new JsonObject() : config.copy(), urls, includedURLs, modDir, parentDeploymentName, mr,
				autoRedeploy, ha, loadFromModuleFirst);
		mr.incRef();

		deployments.put(deploymentID, deployment);

		for (int i = 0; i < instances; i++) {
			// Launch the verticle instance
			Runnable runner = new Runnable() {
				public void run() {
					Verticle verticle;
					try {
						verticle = verticleFactory.createVerticle(theMain);
					} catch (Throwable t) {
						handleDeployFailure(t, deployment, aggHandler);
						return;
					}
					try {
						addVerticle(deployment, verticle, verticleFactory, modID, theMain.className());
						setPathResolver(modDir);
						DefaultFutureResult<Void> vr = new DefaultFutureResult<>();

						verticle.start(vr);
						vr.setHandler(new Handler<AsyncResult<Void>>() {
							@Override
							public void handle(AsyncResult<Void> ar) {
								if (ar.succeeded()) {
									aggHandler.complete();
								} else {
									handleDeployFailure(ar.cause(), deployment, aggHandler);
								}
							}
						});
					} catch (Throwable t) {
						handleDeployFailure(t, deployment, aggHandler);
					}
				}
			};

			if (worker) {
				vertx.startInBackground(runner, multiThreaded);
			} else {
				vertx.startOnEventLoop(runner);
			}
		}
	}

	private void handleDeployFailure(final Throwable t, Deployment dep, final CountingCompletionHandler<Void> handler) {
		// First we undeploy
		doUndeploy(dep, new Handler<AsyncResult<Void>>() {
			public void handle(AsyncResult<Void> res) {
				if (res.failed()) {
					vertx.reportException(res.cause());
				}
				// And pass the *deploy* (not undeploy) Throwable back to the original handler
				handler.failed(t);
			}
		});
	}

	// Must be synchronized since called directly from different thread
	private void addVerticle(Deployment deployment, Verticle verticle, VerticleFactory factory, ModuleIdentifier modID,
			String main) {
		String loggerName = modID + "-" + main + "-" + System.identityHashCode(verticle);
		Logger logger = LoggerFactory.getLogger(loggerName);
		DefaultContext context = vertx.getContext();
		VerticleHolder holder = new VerticleHolder(deployment, context, verticle, loggerName, logger, deployment.config,
				factory);
		deployment.verticles.add(holder);
		context.setDeploymentHandle(holder);
	}

	private VerticleHolder getVerticleHolder() {
		DefaultContext context = vertx.getContext();
		if (context != null) {
			return (VerticleHolder) context.getDeploymentHandle();
		} else {
			return null;
		}
	}

	private void doUndeploy(final Deployment dep, final Handler<AsyncResult<Void>> doneHandler) {
		CountingCompletionHandler<Void> count = new CountingCompletionHandler<>(vertx);
		doUndeploy(dep.name, count);
		count.setHandler(doneHandler);
	}

	private void doUndeploy(String name, final CountingCompletionHandler<Void> parentCount) {
		if (name == null) {
			throw new NullPointerException("deployment id is null");
		}

		final Deployment deployment = deployments.remove(name);
		if (deployment == null) {
			// OK - already undeployed
			parentCount.incRequired();
			parentCount.complete();
			return;
		}

		final CountingCompletionHandler<Void> count = new CountingCompletionHandler<>(vertx);
		parentCount.incRequired();

		// Depth first - undeploy children first
		for (String childDeployment : deployment.childDeployments) {
			doUndeploy(childDeployment, count);
		}

		if (!deployment.verticles.isEmpty()) {
			for (final VerticleHolder holder : deployment.verticles) {
				count.incRequired();
				holder.context.execute(new Runnable() {
					public void run() {
						try {
							holder.verticle.stop();
						} catch (Throwable t) {
							// If an exception is thrown from stop() it's possible the logger system has
							// already shut down so we must
							// report it on stderr too
							System.err.println("Failure in stop()");
							t.printStackTrace();
						}
						LoggerFactory.removeLogger(holder.loggerName);
						holder.context.runCloseHooks(new AsyncResultHandler<Void>() {
							@Override
							public void handle(AsyncResult<Void> asyncResult) {
								holder.context.close();
								runInBackground(new Runnable() {
									public void run() {
										if (deployment.ha && haManager != null) {
											haManager.removeFromHA(deployment.name);
										}
										count.complete();
									}
								}, new Handler<AsyncResult<Void>>() {
									public void handle(AsyncResult<Void> res) {
										if (res.failed()) {
											count.failed(res.cause());
										} else {
											count.complete();
										}
									}
								});
							}
						});
					}
				});
			}
		}

		if (deployment.parentDeploymentName != null) {
			Deployment parent = deployments.get(deployment.parentDeploymentName);
			if (parent != null) {
				parent.childDeployments.remove(name);
			}
		}

		count.setHandler(new Handler<AsyncResult<Void>>() {
			public void handle(AsyncResult<Void> res) {
				deployment.moduleReference.decRef();
				if (res.failed()) {
					parentCount.failed(res.cause());
				} else {
					parentCount.complete();
				}
			}
		});
	}

	private void deleteTmpDeployments() {
		String tmp;
		while ((tmp = tempDeployments.poll()) != null) {
			try {
				vertx.fileSystem().deleteSync(tmp, true);
			} catch (Throwable t) {
				log.error("Failed to delete temp deployment", t);
			}
		}
	}

	public void stop() {
		if (stopped) {
			return;
		}
		if (haManager != null) {
			haManager.stop();
		}
		deleteTmpDeployments();
		vertx.stop();
		stopped = true;
	}

	// For debug only
	public int checkNoModules() {
		int count = 0;
		for (Map.Entry<String, ModuleReference> entry : moduleRefs.entrySet()) {
			if (!entry.getValue().resident) {
				System.out.println("Module remains: " + entry.getKey());
				count++;
			}
		}
		return count;
	}

	public void removeModule(String moduleKey) {
		moduleRefs.remove(moduleKey);
	}

	private static class LanguageImplInfo {
		final String moduleName;
		final String factoryName;

		private LanguageImplInfo(String moduleName, String factoryName) {
			this.moduleName = moduleName;
			this.factoryName = factoryName;
		}

		public String toString() {
			return (moduleName == null ? ":" : (moduleName + ":")) + factoryName;
		}
	}

	static final class ModuleZipInfo {
		final boolean oldStyle;
		final String filename;

		ModuleZipInfo(boolean oldStyle, String filename) {
			this.oldStyle = oldStyle;
			this.filename = filename;
		}
	}

	private static class ModJSONClassLoader extends URLClassLoader {

		private final ClassLoader parent;

		private ModJSONClassLoader(URL[] urls, ClassLoader parent) {
			super(urls, parent);
			this.parent = parent;
		}

		@Override
		public URL getResource(String name) {
			URL url = findResource(name);
			if (url == null) {
				url = super.getResource(name);
			}
			return url;
		}

		@Override
		public Enumeration<URL> getResources(String name) throws IOException {
			List<URL> resources = new ArrayList<>(Collections.list(findResources(name)));
			if (parent != null) {
				resources.addAll(Collections.list(parent.getResources(name)));
			}
			return Collections.enumeration(resources);
		}
	}

}
