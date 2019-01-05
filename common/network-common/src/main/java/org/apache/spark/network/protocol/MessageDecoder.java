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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作用:对从管道中读取的ByteBuf进行解析，防止丢包和解析错误
 * Decoder used by the client side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 *
 * 为什么需要MessageEncoder和MessageDecoder？
 * 因为在基于流的传输里（比如TCP/IP），接收到的数据首先会被存储到一个socket接收缓冲里。
 * 不幸的是，基于流的传输并不是一个数据包队列，而是一个字节队列。即使你发送了2个独立的数据包，
 * 操作系统也不会作为2个消息处理而仅仅认为是一连串的字节。因此不能保证远程写入的数据会被准确地读取。
 * 举个例子，让我们假设操作系统的TCP/TP协议栈已经接收了3个数据包：ABC、DEF、GHI。
 * 由于基于流传输的协议的这种统一的性质，在你的应用程序在读取数据的时候有很高的可能性被分成下面的
 * 片段：AB、CDEFG、H、I。因此，接收方不管是客户端还是服务端，都应该把接收到的数据整理成一个或者多
 * 个更有意义并且让程序的逻辑更好理解的数据。
 */
@ChannelHandler.Sharable
public final class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final Logger logger = LoggerFactory.getLogger(MessageDecoder.class);

  public static final MessageDecoder INSTANCE = new MessageDecoder();

  private MessageDecoder() {}

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    Message.Type msgType = Message.Type.decode(in);
    Message decoded = decode(msgType, in);
    assert decoded.type() == msgType;
    logger.trace("Received message {}: {}", msgType, decoded);
    out.add(decoded);
  }

  private Message decode(Message.Type msgType, ByteBuf in) {
    switch (msgType) {
      case ChunkFetchRequest:
        return ChunkFetchRequest.decode(in);

      case ChunkFetchSuccess:
        return ChunkFetchSuccess.decode(in);

      case ChunkFetchFailure:
        return ChunkFetchFailure.decode(in);

      case RpcRequest:
        return RpcRequest.decode(in);

      case RpcResponse:
        return RpcResponse.decode(in);

      case RpcFailure:
        return RpcFailure.decode(in);

      case OneWayMessage:
        return OneWayMessage.decode(in);

      case StreamRequest:
        return StreamRequest.decode(in);

      case StreamResponse:
        return StreamResponse.decode(in);

      case StreamFailure:
        return StreamFailure.decode(in);

      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }
}
