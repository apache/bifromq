/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.baseenv;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * NettyEnv is a utility class that provides methods to create EventLoopGroup and determine the appropriate channel
 * classes.
 */
public class NettyEnv {
    /**
     * Create an EventLoopGroup based on the availability of Epoll or KQueue.
     *
     * @return An EventLoopGroup instance.
     */
    public static EventLoopGroup createEventLoopGroup(String name) {
        IEnvProvider envProvider = EnvProvider.INSTANCE;
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(envProvider.newThreadFactory(name));
        }
        if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(envProvider.newThreadFactory(name));
        }
        return new NioEventLoopGroup();
    }

    /**
     * Create an EventLoopGroup with a specified number of threads and a custom name.
     *
     * @param nThreads The number of threads in the EventLoopGroup.
     * @param name The name to use for the threads.
     *
     * @return An EventLoopGroup instance.
     */
    public static EventLoopGroup createEventLoopGroup(int nThreads, String name) {
        IEnvProvider envProvider = EnvProvider.INSTANCE;
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(nThreads, envProvider.newThreadFactory(name));
        }
        if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(nThreads, envProvider.newThreadFactory(name));
        }
        return new NioEventLoopGroup(nThreads, envProvider.newThreadFactory(name));
    }

    /**
     * Get the appropriate SocketChannel class based on the availability of Epoll or KQueue.
     *
     * @return The SocketChannel class.
     */
    public static Class<? extends SocketChannel> getSocketChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

    /**
     * Get the appropriate ServerSocketChannel class based on the availability of Epoll or KQueue.
     *
     * @return The ServerSocketChannel class.
     */
    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollServerSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    /**
     * Determine the appropriate SocketChannel class based on the provided EventLoopGroup.
     *
     * @param eventLoopGroup  The EventLoopGroup to check.
     *
     * @return The SocketChannel class.
     */
    public static Class<? extends SocketChannel> determineSocketChannelClass(EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollSocketChannel.class;
        }
        if (eventLoopGroup instanceof KQueueEventLoopGroup) {
            return KQueueSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

    /**
     * Determine the appropriate ServerSocketChannel class based on the provided EventLoopGroup.
     *
     * @param eventLoopGroup The EventLoopGroup to check.
     * @return The ServerSocketChannel class.
     */
    public static Class<? extends ServerSocketChannel> determineServerSocketChannelClass(
        EventLoopGroup eventLoopGroup) {
        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            return EpollServerSocketChannel.class;
        }
        if (eventLoopGroup instanceof KQueueEventLoopGroup) {
            return KQueueServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    /**
     * Get the appropriate DatagramChannel class based on the availability of Epoll or KQueue.
     *
     * @return The DatagramChannel class.
     */
    public static Class<? extends DatagramChannel> getDatagramChannelClass() {
        if (Epoll.isAvailable()) {
            return EpollDatagramChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueDatagramChannel.class;
        }
        return NioDatagramChannel.class;
    }
}
