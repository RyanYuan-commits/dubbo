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
package org.apache.dubbo.common.serialize;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 序列化策略接口，用于获取特定的序列化与反序列化器的实现。
 * 默认扩展为 hessian2，也是 Dubbo 协议的默认序列化实现。
 * 例如：&lt;dubbo:protocol serialization="xxx" /&gt;
 */
@SPI("hessian2")
public interface Serialization {

    /**
     * 获取序列化实现类的唯一标识 ID，已有的实现在 {@link Constants} 中记录；
     * 新增时主要不要与已有的重复，且不要大于 ExchangeCodec.SERIALIZATION_MASK (31)，
     * 因为 Dubbo 协议在消息头中使用 5 位来记录序列化 ID。
     *
     * @return 内容类型 ID
     */
    byte getContentTypeId();

    /**
     * 获取序列化实现的名称，如 <code>avro/binary</code>
     *
     * @return 序列化实现名称
     */
    String getContentType();

    /**
     * 获取序列化实现实例
     *
     * @param url 远程服务的 URL 地址
     * @param output 底层输出流
     * @return 序列化器
     * @throws IOException 当发生 IO 异常时抛出
     */
    @Adaptive
    ObjectOutput serialize(URL url, OutputStream output) throws IOException;

    /**
     * 获取反序列化实现实例
     *
     * @param url 远程服务的 URL 地址
     * @param input 底层输入流
     * @return 反序列化器
     * @throws IOException 当发生 IO 异常时抛出
     */
    @Adaptive
    ObjectInput deserialize(URL url, InputStream input) throws IOException;

}
