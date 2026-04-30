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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

import java.util.Collections;
import java.util.List;

/**
 * Protocol. (API/SPI, Singleton, ThreadSafe)
 * <p>
 * 关键的装饰器实现：
 * <ol>
 *     <li> {@link org.apache.dubbo.rpc.protocol.ProtocolListenerWrapper} </li>
 *     <li> {@link org.apache.dubbo.rpc.protocol.ProtocolFilterWrapper} </li>
 * </ol>
 */
@SPI("dubbo")
public interface Protocol {

    /**
     * 获取默认端口
     *
     * @return default port
     */
    int getDefaultPort();

    /**
     * 将一个 Invoker 暴露出去
     *
     * <ol>
     *     <li> 在收到一个请求后，Protocol 需要记录请求的原地址：RpcContext.getContext().setRemoteAddress() </li>
     *     <li> export() 方法的实现需要是幂等的，即同一个服务多次调用 export() 方法，结果是相同的。</li>
     *     <li> Invoker 实例在框架内部实现，Protocol 无需关心 </li>
     * </ol>
     *
     * @param <T>     Service type
     * @param invoker Service invoker
     * @return 暴露服务对应的 Exporter 实例，可以用于服务的取消暴露
     * @throws RpcException thrown when error occurs during export the service, for example: port is occupied
     */
    @Adaptive
    <T> Exporter<T> export(Invoker<T> invoker) throws RpcException;

    /**
     * 引用一个 Invoker，refer() 方法会根据参数返回一个 Invoker 对象
     *
     * <ol>
     *     <li> 当用户调用通过 refer() 方法返回的 Invoker 的 invoke() 方法时，Protocol 需要对应的调用对端的 invoke() 方法 </li>
     *     <li> Protocol 应当实现通过 refer() 方法返回的 Invoker，简单来说，Protocol 请求对端服务。 </li>
     *     <li> 当 URL 中的 check=false 时，Protocol 在连接异常时，应该尝试修复，而不是抛出异常 </li>
     * </ol>
     *
     * @param <T>  Service type
     * @param type Service class
     * @param url  URL address for the remote service
     * @return invoker service's local proxy
     * @throws RpcException when there's any error while connecting to the service provider
     */
    @Adaptive
    <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException;

    /**
     * 销毁 Protocol
     *
     * <ol>
     *     <li> 取消所有通过该 Protocol 暴露的服务 </li>
     *     <li> 释放所有该 Protocol 占用的资源，如连接、端口等 </li>
     *     <li> Protocol 可以在销毁后继续暴露新的服务 </li>
     * </ol>
     */
    void destroy();

    /**
     * 获取所有通过该 Protocol 暴露的服务
     */
    default List<ProtocolServer> getServers() {
        return Collections.emptyList();
    }

}