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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.extension.SPI;


/**
 * ChannelHandler. (API, Prototype, ThreadSafe)
 * 注册在 Endpoint 上的消息处理器, 方法命名为过去式, 处理的是已发生的事件。
 * <p>
 * 有三条继承链路
 * <ol>
 *     <li>
 *         {@link org.apache.dubbo.remoting.transport.ChannelHandlerDispatcher ChannelHandlerDispatcher}：
 *         存储多个 channel handler，事件触发后，分别调用 channel handler 的对应方法；
 *     </li>
 *     <li>
 *         {@link org.apache.dubbo.remoting.transport.ChannelHandlerAdapter ChannelHandlerAdapter}：
 *         空实现，TelnetHandlerAdapter 继承并实现该类；
 *     </li>
 *     <li>
 *         {@link org.apache.dubbo.remoting.transport.ChannelHandlerDelegate ChannelhandlerDelegate}：
 *         对另一个 channel 的封装，用于对消息做处理或分发。
 *     </li>
 * </ol>
 *
 * @see org.apache.dubbo.remoting.Transporter#bind(org.apache.dubbo.common.URL, ChannelHandler)
 * @see org.apache.dubbo.remoting.Transporter#connect(org.apache.dubbo.common.URL, ChannelHandler)
 */
@SPI
public interface ChannelHandler {

    void connected(Channel channel) throws RemotingException;

    void disconnected(Channel channel) throws RemotingException;

    void sent(Channel channel, Object message) throws RemotingException;

    void received(Channel channel, Object message) throws RemotingException;

    void caught(Channel channel, Throwable exception) throws RemotingException;

}