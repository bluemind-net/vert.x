/* BEGIN LICENSE
  * Copyright © Blue Mind SAS, 2012-2018
  *
  * This file is part of BlueMind. BlueMind is a messaging and collaborative
  * solution.
  *
  * This program is free software; you can redistribute it and/or modify
  * it under the terms of either the GNU Affero General Public License as
  * published by the Free Software Foundation (version 3 of the License).
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  *
  * See LICENSE.txt
  * END LICENSE
  */
package org.vertx.java.core;

import org.vertx.java.core.impl.VertxThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public final class ChannelClasses {

	public static Class<? extends ServerChannel> serverSocket() {
		return NioServerSocketChannel.class;
	}

	public static Class<? extends Channel> clientSocket() {
		return NioSocketChannel.class;
	}

	public static EventLoopGroup createLoopGroup(int threads, String poolName) {
		return new NioEventLoopGroup(threads, new VertxThreadFactory(poolName));
	}

}
