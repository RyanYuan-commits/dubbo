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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.support.FailoverCluster;

/**
 * 集群容错接口，在某些 Provider 节点发生故障时，让 Consumer 的调用能够发送到正常的 Provider 节点
 * Cluster. (SPI, Singleton, ThreadSafe)
 * <p>
 * 当调用进入 Cluster 的时候，Cluster 会创建一个 AbstractClusterInvoker 对象，在这个 AbstractClusterInvoker 中，首先会
 * 从 Directory 中获取当前 Invoker 集合；然后按照 Router 集合进行路由，得到符合条件的 Invoker 集合；接下来按照
 * LoadBalance 指定的负载均衡策略得到最终要调用的 Invoker 对象。
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Computer_cluster">Cluster</a>
 * <a href="http://en.wikipedia.org/wiki/Fault-tolerant_system">Fault-Tolerant</a>
 * <p>
 *  Cluster 有两条继承链路：
 *  <ol>
 *      <li> {@link org.apache.dubbo.rpc.cluster.support.wrapper.MockClusterWrapper} </li>
 *      <li> {@link org.apache.dubbo.rpc.cluster.support.wrapper.AbstractCluster} </li>
 *  </ol>
 */
@SPI(Cluster.DEFAULT)
public interface Cluster {

    String DEFAULT = FailoverCluster.NAME;

    /**
     * 将 directory 中的 invoker 合并为一个虚拟 invoker
     *
     * @return cluster invoker
     */
    @Adaptive
    <T> Invoker<T> join(Directory<T> directory) throws RpcException;

    static Cluster getCluster(String name) {
        return getCluster(name, true);
    }

    static Cluster getCluster(String name, boolean wrap) {
        if (StringUtils.isEmpty(name)) {
            name = Cluster.DEFAULT;
        }
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension(name, wrap);
    }
}