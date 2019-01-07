/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.spark.network.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * 作用:RPC框架的服务端，提供高效的、低级别的流服务。
 * Server for the efficient, low-level streaming service.
 */
public class TransportServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private final TransportContext context;
    private final TransportConf conf;
    private final RpcHandler appRpcHandler;
    private final List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;

    /**
     * Creates a TransportServer that binds to the given host and the given port, or to any available
     * if 0. If you don't want to bind to any special host, set "hostToBind" to null.
     */
    public TransportServer(
            TransportContext context,
            String hostToBind,
            int portToBind,
            RpcHandler appRpcHandler,
            List<TransportServerBootstrap> bootstraps) {
        this.context = context;
        this.conf = context.getConf();
        this.appRpcHandler = appRpcHandler;
        this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

        try {
            init(hostToBind, portToBind);
        } catch (RuntimeException e) {
            // 关闭退出
            JavaUtils.closeQuietly(this);
            throw e;
        }
    }

    public int getPort() {
        if (port == -1) {
            throw new IllegalStateException("Server not initialized");
        }
        return port;
    }

    private void init(String hostToBind, int portToBind) {
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        /**
         * boss 线程组：用于服务端接受客户端的连接。
         * worker 线程组：用于进行客户端的 SocketChannel 的数据读写。
         *
         * bossGroup 是用于服务端 的 accept 的, 即用于处理客户端的连接请求. 我们可以把 Netty 比作一个饭店,
         * bossGroup 就像一个像一个前台接待, 当客户来到饭店吃时, 接待员就会引导顾客就坐, 为顾客端茶送水等. 而
         * workerGroup, 其实就是实际上干活的啦, 它们负责客户端连接通道的 IO 操作: 当接待员 招待好顾客后, 就可
         * 以稍做休息, 而此时后厨里的厨师们(workerGroup)就开始忙碌地准备饭菜了.
         *
         * 其它参考:https://www.jianshu.com/p/128ddc36e713
         */
        EventLoopGroup bossGroup =
                NettyUtils.createEventLoop(ioMode, conf.serverThreads(), conf.getModuleName() + "-server");
        EventLoopGroup workerGroup = bossGroup;

        // 基于内存池的 ByteBuf 的分配器
        PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
                conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());
        // 创建ServerBootstrap
        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NettyUtils.getServerChannelClass(ioMode))
                // 设置 NioServerSocketChannel的可选项
                .option(ChannelOption.ALLOCATOR, allocator)
                .childOption(ChannelOption.ALLOCATOR, allocator);

        if (conf.backLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
        }

        if (conf.receiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
        }

        if (conf.sendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
        }
        // 设置子处理器
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                RpcHandler rpcHandler = appRpcHandler;
                for (TransportServerBootstrap bootstrap : bootstraps) {
                    rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
                }
                context.initializePipeline(ch, rpcHandler);
            }
        });

        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind) : new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
        // 同步不可中断
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        logger.debug("Shuffle server started on port: {}", port);
    }

    @Override
    public void close() {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.group() != null) {
            bootstrap.group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.childGroup() != null) {
            bootstrap.childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }
}
