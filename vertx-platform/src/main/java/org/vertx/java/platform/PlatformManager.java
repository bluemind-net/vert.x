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

package org.vertx.java.platform;

import java.net.URL;
import java.util.Map;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 *
 * Represents the Vert.x platform.
 * <p>
 * It's the role of a PlatformManager to deploy and undeploy modules and
 * verticles. It's also used to install modules, and for various other tasks.
 * <p>
 * The Platform Manager basically represents the Vert.x container in which
 * verticles and modules run.
 * <p>
 * The Platform Manager is used by the Vert.x CLI to run/install/etc modules and
 * verticles but you could also use it if you want to embed the entire Vert.x
 * container in an application, or write some other tool (e.g. a build tool, or
 * a test tool) which needs to do stuff with the container.
 * <p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public interface PlatformManager {

	/**
	 * Deploy a verticle
	 * 
	 * @param main        The main, e.g. app.js, foo.rb, org.mycompany.MyMain, etc
	 * @param config      Any JSON config to pass to the verticle, or null if none
	 * @param classpath   The classpath for the verticle
	 * @param instances   The number of instances to deploy
	 * @param includes    Comma separated list of modules to include, or null if
	 *                    none
	 * @param doneHandler Handler will be called with deploymentID when deployed, or
	 *                    null if it fails to deploy
	 */
	void deployVerticle(VerticleConstructor main, JsonObject config, URL[] classpath, int instances, String includes,
			Handler<AsyncResult<String>> doneHandler);

	/**
	 * Deploy a worker verticle
	 * 
	 * @param multiThreaded Is it a multi-threaded worker verticle?
	 * @param main          The main, e.g. app.js, foo.rb, org.mycompany.MyMain, etc
	 * @param config        Any JSON config to pass to the verticle, or null if none
	 * @param classpath     The classpath for the verticle
	 * @param instances     The number of instances to deploy
	 * @param includes      Comma separated list of modules to include, or null if
	 *                      none
	 * @param doneHandler   Handler will be called with deploymentID when deployed,
	 *                      or null if it fails to deploy
	 */
	void deployWorkerVerticle(boolean multiThreaded, VerticleConstructor main, JsonObject config, URL[] classpath,
			int instances, String includes, Handler<AsyncResult<String>> doneHandler);

	/**
	 * Undeploy a deployment
	 * 
	 * @param deploymentID The ID of the deployment to undeploy, as given in the
	 *                     doneHandler when deploying
	 * @param doneHandler  The done handler will be called when deployment is
	 *                     complete or fails
	 */
	void undeploy(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

	/**
	 * Undeploy all verticles and modules
	 * 
	 * @param doneHandler The done handler will be called when complete
	 */
	void undeployAll(Handler<AsyncResult<Void>> doneHandler);

	/**
	 * List all deployments, with deployment ID and number of instances
	 * 
	 * @return map of instances
	 */
	Map<String, Integer> listInstances();

	/**
	 * Pull in all the dependencies (the 'includes' and the 'deploys' fields in
	 * mod.json) and copy them into an internal mods directory in the module. This
	 * allows a self contained module to be created.
	 * 
	 * @param moduleName The name of the module
	 */
	void pullInDependencies(String moduleName, Handler<AsyncResult<Void>> doneHandler);

	/**
	 * Create a fat executable jar which includes the Vert.x binaries and the module
	 * so it can be run directly with java without having to pre-install Vert.x.
	 * e.g.: java -jar mymod~1.0-fat.jar
	 * 
	 * @param moduleName      The name of the module to create the fat jar for
	 * @param outputDirectory Directory in which to place the jar
	 * @param doneHandler     Handler that will be called on completion
	 */
	void makeFatJar(String moduleName, String outputDirectory, Handler<AsyncResult<Void>> doneHandler);

	/**
	 * Register a handler that will be called when the platform exits because of a
	 * verticle calling container.exit()
	 * 
	 * @param handler The handler
	 */
	void registerExitHandler(Handler<Void> handler);

	/**
	 * @return A reference to the Vertx instance used by the platform manager
	 */
	Vertx vertx();

	/**
	 * Stop the platform manager
	 */
	void stop();

}
