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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * 对象输入接口。
 */
public interface ObjectInput extends DataInput {

    /**
     * 尽可能使用 {@link #readObject(Class)} 或 {@link #readObject(Class, Type)}
     *
     * @return 对象
     * @throws IOException 如果发生 I/O 错误
     * @throws ClassNotFoundException 如果发生类未找到异常
     */
    @Deprecated
    Object readObject() throws IOException, ClassNotFoundException;

    /**
     * 读取对象
     *
     * @param cls 对象类
     * @return 对象
     * @throws IOException 如果发生 I/O 错误
     * @throws ClassNotFoundException 如果发生类未找到异常
     */
    <T> T readObject(Class<T> cls) throws IOException, ClassNotFoundException;

    /**
     * 读取对象
     *
     * @param cls 对象类
     * @param type 对象类型
     * @return 对象
     * @throws IOException 如果发生 I/O 错误
     * @throws ClassNotFoundException 如果发生类未找到异常
     */
    <T> T readObject(Class<T> cls, Type type) throws IOException, ClassNotFoundException;


    /**
     * 以下方法是为满足 Dubbo RPC 协议实现的需求而定制的。旧版协议实现会尝试直接将 Map、Throwable 和 Null 值写入流中，
     * 这并不符合所有序列化协议的限制。
     *
     * <p>
     * 有关更多详细信息，请参阅 ProtobufSerialization、KryoSerialization 如何实现这些方法。
     * </p>
     *
     * 将 RPC 协议与业务序列化协议绑定并不是一个好的实践。RPC 协议的编码应高度独立且可移植，易于跨平台和语言，
     * 例如像 HTTP 头一样，将头/附件的内容限制为 ASCII 字符串，并使用 ISO_8859_1 对其进行编码。
     */
    default Throwable readThrowable() throws IOException, ClassNotFoundException {
        Object obj = readObject();
        if (!(obj instanceof Throwable)) {
            throw new IOException("Response data error, expect Throwable, but get " + obj);
        }
        return (Throwable) obj;
    }

    default Object readEvent() throws IOException, ClassNotFoundException {
        return readObject();
    }

    default Map<String, Object> readAttachments() throws IOException, ClassNotFoundException {
        return readObject(Map.class);
    }

}