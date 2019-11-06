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

import java.util.Map;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.VerticleConstructor;

public class DefaultContainer implements Container {

	private final PlatformManagerInternal mgr;

	DefaultContainer(final PlatformManagerInternal mgr) {
		this.mgr = mgr;
	}

	@Override
	public void deployWorkerVerticle(VerticleConstructor main) {
		deployWorkerVerticle(main, null, 1);
	}

	@Override
	public void deployWorkerVerticle(VerticleConstructor main, int instances) {
		deployWorkerVerticle(main, null, instances);
	}

	@Override
	public void deployWorkerVerticle(VerticleConstructor main, JsonObject config) {
		deployWorkerVerticle(main, config, 1);
	}

	@Override
	public void deployWorkerVerticle(VerticleConstructor main, JsonObject config, int instances) {
		deployWorkerVerticle(main, config, instances, false, null);
	}

	@Override
	public void deployWorkerVerticle(VerticleConstructor main, JsonObject config, int instances,
			boolean multiThreaded) {
		deployWorkerVerticle(main, config, instances, multiThreaded, null);
	}

	@Override
	public void deployWorkerVerticle(VerticleConstructor main, JsonObject config, int instances, boolean multiThreaded,
			Handler<AsyncResult<String>> doneHandler) {
		mgr.deployWorkerVerticle(multiThreaded, main, config, null, instances, null, doneHandler);
	}

	@Override
	public void deployVerticle(VerticleConstructor main) {
		deployVerticle(main, null, 1);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, int instances) {
		deployVerticle(main, null, instances);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, JsonObject config) {
		deployVerticle(main, config, 1);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, JsonObject config, int instances) {
		deployVerticle(main, config, instances, null);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, JsonObject config, int instances,
			Handler<AsyncResult<String>> doneHandler) {
		mgr.deployVerticle(main, config, null, instances, null, doneHandler);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, Handler<AsyncResult<String>> doneHandler) {
		this.deployVerticle(main, null, 1, doneHandler);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, JsonObject config, Handler<AsyncResult<String>> doneHandler) {
		this.deployVerticle(main, config, 1, doneHandler);
	}

	@Override
	public void deployVerticle(VerticleConstructor main, int instances, Handler<AsyncResult<String>> doneHandler) {
		this.deployVerticle(main, null, instances, doneHandler);
	}

	@Override
	public void undeployVerticle(String deploymentID) {
		undeployVerticle(deploymentID, null);
	}

	@Override
	public void undeployVerticle(String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
		mgr.undeploy(deploymentID, doneHandler);
	}

	@Override
	public JsonObject config() {
		return mgr.config();
	}

	@Override
	public Logger logger() {
		return mgr.logger();
	}

	@Override
	public void exit() {
		mgr.exit();
	}

	@Override
	public Map<String, String> env() {
		return System.getenv();
	}

}
