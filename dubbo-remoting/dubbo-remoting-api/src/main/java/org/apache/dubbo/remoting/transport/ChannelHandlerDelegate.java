/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.remoting.ChannelHandler;

/**
 * 对另一个 ChannelHandler 接口的封装，有两个继承链路：
 * <ol>
 *     <li> {@link AbstractChannelHandlerDelegate}：在将消息转发给 channel handler 之前做处理，如解码、心跳等； </li>
 *     <li> {@link org.apache.dubbo.remoting.transport.dispatcher.WrappedChannelHandler WrappedChannelHandler}：消息派发机制的实现。 </li>
 * </ol>
 */
public interface ChannelHandlerDelegate extends ChannelHandler {

    ChannelHandler getHandler();

}
