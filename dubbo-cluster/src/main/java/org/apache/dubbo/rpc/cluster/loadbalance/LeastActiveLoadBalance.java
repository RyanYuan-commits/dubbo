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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance，通过最小的活跃调用数过滤 invoker
 * 需要搭配 {@link org.apache.dubbo.rpc.filter.ActiveLimitFilter} 使用
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 初始化Invoker数量
        int length = invokers.size();
        // 记录最小的活跃请求数
        int leastActive = -1;
        // 记录活跃请求数最小的Invoker集合的个数
        int leastCount = 0;
        // 记录活跃请求数最小的Invoker在invokers数组中的下标位置
        int[] leastIndexes = new int[length];
        // 记录活跃请求数最小的Invoker集合中，每个Invoker的权重值
        int[] weights = new int[length];
        // 记录活跃请求数最小的Invoker集合中，所有Invoker的权重值之和
        int totalWeight = 0;
        // 记录活跃请求数最小的Invoker集合中，第一个Invoker的权重值
        int firstWeight = 0;
        // 活跃请求数最小的集合中，所有Invoker的权重值是否相同
        boolean sameWeight = true;

        // Filter out all the least active invokers
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 获取该Invoker的活跃请求数
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            // 获取该Invoker的权重，默认值为 100
            int afterWarmup = getWeight(invoker, invocation);
            weights[i] = afterWarmup;
            // 比较活跃请求数
            if (leastActive == -1 || active < leastActive) {
                // 当前的Invoker是第一个活跃请求数最小的Invoker，则记录如下信息
                leastActive = active;
                leastCount = 1;
                leastIndexes[0] = i;
                totalWeight = afterWarmup;
                firstWeight = afterWarmup;
                sameWeight = true;
            } else if (active == leastActive) {
                // 当前 invoker 属于活跃数最小的集合
                leastIndexes[leastCount++] = i;
                totalWeight += afterWarmup;
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        if (leastCount == 1) {
            // 只有一个活跃数最小的 invoker，直接返回即可
            return invokers.get(leastIndexes[0]);
        }
        if (!sameWeight && totalWeight > 0) {
            // 按照 RandomLoadBalance 的逻辑，从活跃请求数最小的 Invoker 集合中，随机选择一个 Invoker 对象返回
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }

        // 所有 invoker 权重相同，随机返回一个
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
