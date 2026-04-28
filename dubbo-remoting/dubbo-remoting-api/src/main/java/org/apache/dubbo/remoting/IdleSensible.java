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

/**
 * 说明服务端与客户端的实现方案是否具备检测并处理空闲连接的能力。
 * <ul>
 *     <li>若服务端支持空闲连接处理，需在连接进入空闲状态时主动断开连接；</li>
 *     <li>若客户端支持空闲连接处理，则需向服务端持续发送心跳包。</li>
 * </ul>
 */
public interface IdleSensible {

    /**
     * 是否能够自主处理空闲连接，默认为 false，实现类可以自己构建一个 timer 来处理空闲连接
     *
     * @return 是否能够自主处理空闲连接
     */
    default boolean canHandleIdle() {
        return false;
    }

}
