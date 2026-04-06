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
package org.apache.dubbo.common.serialize.support;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 提供统一的序列化注册表，此类用于 {@code dubbo-serialization-fst}
 * 和 {@code dubbo-serialization-kryo}，它将在启动时注册一些类（例如 {@link AbstractKryoFactory#create}）
 */
public abstract class SerializableClassRegistry {

    private static final Map<Class<?>, Object> REGISTRATIONS = new LinkedHashMap<>();

    /**
     * 仅应在启动时调用
     *
     * @param clazz 对象类型
     */
    public static void registerClass(Class<?> clazz) {
        registerClass(clazz, null);
    }

    /**
     * 仅应在启动时调用
     *
     * @param clazz 对象类型
     * @param serializer 对象序列化器
     */
    public static void registerClass(Class<?> clazz, Object serializer) {
        if (clazz == null) {
            throw new IllegalArgumentException("注册到 kryo 的类不能为空！");
        }
        REGISTRATIONS.put(clazz, serializer);
    }

    /**
     * 获取已注册的类
     *
     * @return 类序列化器映射
     * */
    public static Map<Class<?>, Object> getRegisteredClasses() {
        return REGISTRATIONS;
    }

}
