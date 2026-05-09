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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 该类从多个服务提供者中随机选择一个。
 * 你可以为每个提供者设置权重：
 * 如果所有提供者权重相同，则直接使用 random.nextInt(提供者数量) 随机选择。
 * 如果权重不同，则使用 random.nextInt(权重1 + 权重2 + ... + 权重n) 进行加权随机选择。
 * <p>
 * 注意：如果某台机器性能优于其他机器，可以为其设置更大的权重；
 * 如果机器性能一般，则可以设置较小的权重。
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    /**
     * 用随机准则在 invoker list 中选择一个 invoker
     *
     * @param invokers List of possible invokers
     * @param url URL
     * @param invocation Invocation
     * @param <T>
     * @return The selected invoker
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size();
        boolean sameWeight = true;
        // 计算每一个 invoker 对象对应的权重，并填充到 weights[] 数组
        int[] weights = new int[length];
        // 第一个 invoker 的权重
        int firstWeight = getWeight(invokers.get(0), invocation);
        weights[0] = firstWeight;

        // 记录总权重值
        int totalWeight = firstWeight;
        for (int i = 1; i < length; i++) {
            // 计算每一个 Invoker 的权重
            int weight = getWeight(invokers.get(i), invocation);
            weights[i] = weight;
            totalWeight += weight;
            if (sameWeight && weight != firstWeight) {
                // 检测每一个 Provider 的权重是否相同
                sameWeight = false;
            }
        }

        if (totalWeight > 0 && !sameWeight) {
            // invoker 权重值不全相等，计算随机数落在哪个区间上
            // 随机获取一个 [0, totalWeight) 的数字
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // 循环让offset数减去Invoker的权重值，当offset小于0时，返回相应的Invoker
            for (int i = 0; i < length; i++) {
                offset -= weights[i];
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }

        // 各个 invoker 权重值相同，随机返回一个 invoker 即可
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }

}
