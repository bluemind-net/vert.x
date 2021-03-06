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

import org.vertx.java.core.Vertx;
import org.vertx.java.core.logging.Logger;

/**
 * Used by the platform manager to create Verticle instances.
 * <p>
 *
 * Each language module implementation will provide an instance of this class
 * and the platform will use it to instantiate Verticle instances for that
 * language.
 * <p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public interface VerticleFactory {

	void init(Vertx vertx, Container container, ClassLoader cl);

	// Verticle createVerticle(String main) throws Exception;

	Verticle createVerticle(VerticleConstructor ctor) throws Exception;

	void reportException(Logger logger, Throwable t);

	void close();

}
