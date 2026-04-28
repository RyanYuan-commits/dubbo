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

import org.apache.dubbo.common.extension.SPI;

/**
 * 用于拦截 Provider 和 Consumer 的调用过程；此外 Dubbo 中的大多数功能都是基于同一套 Filter 机制实现的。<br>
 * 每次 invoke 都会执行完整的过滤器链路，在增加过滤器前需要考虑其损耗。<br>
 *
 * <pre>
 *  从执行时序上来看，过滤器的工作方式为：
 *    <b>
 *    ... 过滤器前置逻辑 ...
 *          invoker.invoke(invocation) // 进入下一个 Filter 或者执行真正的调用
 *    ... 过滤器后置逻辑 ...
 *    </b>
 *    Dubbo 中的缓存功能就是通过过滤器方式实现的。如果为调用配置了缓存，那么在远程调用之前，
 *    会先调用所配置的缓存类型（例如：Thread Local、Jcache 等）的实现类中的 invoke 方法。
 * </pre>
 *
 * Filter. (SPI, Singleton, ThreadSafe)
 *
 * @see org.apache.dubbo.rpc.filter.GenericFilter
 * @see org.apache.dubbo.rpc.filter.EchoFilter
 * @see org.apache.dubbo.rpc.filter.TokenFilter
 * @see org.apache.dubbo.rpc.filter.TpsLimitFilter
 */
@SPI
public interface Filter {

    /**
     * 确保实现中存在 invoker.invoke() 方法的调用
     */
    Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException;

    interface Listener {

        void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation);

        void onError(Throwable t, Invoker<?> invoker, Invocation invocation);
    }

}