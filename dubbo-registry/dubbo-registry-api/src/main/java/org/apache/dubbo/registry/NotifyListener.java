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
package org.apache.dubbo.registry;

import org.apache.dubbo.common.URL;

import java.util.List;

/**
 * NotifyListener. (API, Prototype, ThreadSafe)
 *
 * @see org.apache.dubbo.registry.RegistryService#subscribe(URL, NotifyListener)
 */
public interface NotifyListener {

    /**
     * 接收到服务变更通知时触发，监听 providers、configurators 和 routers 三个目录
     * <p>
     * notify 方法必须遵循以下约定：<br>
     * 1. 始终以【服务接口 + 数据类型】为维度进行通知。即：不会只通知同一个服务下相同类型数据的部分内容，
     *    用户无需对比上一次的通知结果。<br>
     * 2. 订阅后的第一次通知，必须是该服务下所有数据类型的全量通知。<br>
     * 3. 数据发生变更时，允许分类型单独通知不同数据，例如：服务提供者、服务消费者、路由规则、覆盖配置。
     *    允许仅通知其中一种类型，但该类型的数据必须是全量的，而非增量的。<br>
     * 4. 如果某个数据类型为空，必须通知一条携带分类参数标识的空 URL 协议数据。<br>
     * 5. 通知的顺序必须由通知方（即注册中心的实现）保证。例如：单线程推送、队列序列化、版本对比。<br>
     *
     * @param urls 注册信息列表，永远不为空。其含义与 {@link org.apache.dubbo.registry.RegistryService#lookup(URL)}
     *             方法的返回值完全一致。
     */

    void notify(List<URL> urls);

}