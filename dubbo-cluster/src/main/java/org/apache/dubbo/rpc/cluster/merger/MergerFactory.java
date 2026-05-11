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

package org.apache.dubbo.rpc.cluster.merger;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.cluster.Merger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @see org.apache.dubbo.rpc.cluster.support.MergeableClusterInvoker
 */
public class MergerFactory {

    private static final ConcurrentMap<Class<?>, Merger<?>> MERGER_CACHE =
            new ConcurrentHashMap<Class<?>, Merger<?>>();

    /**
     * 根据返回值类型选择合适的 merger
     *
     * @param returnType 从 merger 会得到这个类型的结果
     * @return the merger which merges an array of returnType into one, return null if not exist
     * @throws IllegalArgumentException if returnType is null
     */
    public static <T> Merger<T> getMerger(Class<T> returnType) {
        if (returnType == null) {
            // returnType 为空，直接抛出异常
            throw new IllegalArgumentException("returnType is null");
        }

        Merger result;
        if (returnType.isArray()) {
            // 数组类型的 return type
            Class type = returnType.getComponentType();
            // 获取元素对应的 Merger 实现
            result = MERGER_CACHE.get(type);
            if (result == null) {
                loadMergers();
                result = MERGER_CACHE.get(type);
            }
            // 如果 Dubbo 没有提供元素类型对应的 Merger 实现，就返回 ArrayMerger
            if (result == null && !type.isPrimitive()) {
                result = ArrayMerger.INSTANCE;
            }
        } else {
            // 从缓存中查找对应的 Merger 实例
            result = MERGER_CACHE.get(returnType);
            if (result == null) {
                loadMergers();
                result = MERGER_CACHE.get(returnType);
            }
        }
        return result;
    }

    static void loadMergers() {
        // 获取 Merger 接口的所有扩展名称
        Set<String> names = ExtensionLoader.getExtensionLoader(Merger.class)
                .getSupportedExtensions();
        for (String name : names) {
            // 遍历所有 Merger 扩展实现
            Merger m = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(name);
            MERGER_CACHE.putIfAbsent(ReflectUtils.getGenericClass(m.getClass()), m);
        }
    }

}
