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

import java.net.InetSocketAddress;

/**
 * Channel. (API/SPI, Prototype, ThreadSafe)
 * 有状态的链接, 具备存储 k-v 的能力
 *
 * @see org.apache.dubbo.remoting.Client
 * @see RemotingServer#getChannels()
 * @see RemotingServer#getChannel(InetSocketAddress)
 */
public interface Channel extends Endpoint {

    InetSocketAddress getRemoteAddress();

    /**
     * 当前 channel 是否处于连接状态
     *
     * @return connected
     */
    boolean isConnected();

    // channel 具备存储 k-v 的能力
    boolean hasAttribute(String key);

    Object getAttribute(String key);

    void setAttribute(String key, Object value);

    void removeAttribute(String key);

}