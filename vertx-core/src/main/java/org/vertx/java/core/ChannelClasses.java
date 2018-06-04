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

import java.io.File;

import org.vertx.java.core.impl.VertxThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public final class ChannelClasses {

  private static boolean useEpoll() {
    return Epoll.isAvailable() && new File("/etc/bm/use.epoll").exists();
  }

  public static Class<? extends ServerChannel> serverSocket() {
    if (useEpoll()) {
      return EpollServerSocketChannel.class;
    } else {
      return NioServerSocketChannel.class;
    }
  }

  public static Class<? extends Channel> clientSocket() {
    if (useEpoll()) {
      return EpollSocketChannel.class;
    } else {
      return NioSocketChannel.class;
    }
  }

  public static Class<? extends DatagramChannel> datagramChannel() {
    if (useEpoll()) {
      return EpollDatagramChannel.class;
    } else {
      return NioDatagramChannel.class;
    }
  }

  public static DatagramChannel datagramSocket(org.vertx.java.core.datagram.InternetProtocolFamily family) {
    if (useEpoll()) {
      return new EpollDatagramChannel();
    } else {
      if (family == null) {
        return new NioDatagramChannel();
      }
      switch (family) {
      case IPv4:
        return new NioDatagramChannel(InternetProtocolFamily.IPv4);
      case IPv6:
        return new NioDatagramChannel(InternetProtocolFamily.IPv6);
      default:
        return new NioDatagramChannel();
      }
    }
  }

  public static EventLoopGroup createLoopGroup(int threads, String poolName) {
    if (useEpoll()) {
      return new EpollEventLoopGroup(threads, new VertxThreadFactory(poolName));
    } else {
      return new NioEventLoopGroup(threads, new VertxThreadFactory(poolName));
    }
  }

}
