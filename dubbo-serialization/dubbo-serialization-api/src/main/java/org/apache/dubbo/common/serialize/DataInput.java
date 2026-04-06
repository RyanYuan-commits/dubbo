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

/**
 * 基本数据类型输入接口。
 */
public interface DataInput {

    /**
     * 读取布尔值。
     *
     * @return 布尔值。
     * @throws IOException IO异常
     */
    boolean readBool() throws IOException;

    /**
     * 读取字节。
     *
     * @return 字节值。
     * @throws IOException IO异常
     */
    byte readByte() throws IOException;

    /**
     * 读取短整型。
     *
     * @return 短整型值。
     * @throws IOException IO异常
     */
    short readShort() throws IOException;

    /**
     * 读取整型。
     *
     * @return 整型值。
     * @throws IOException IO异常
     */
    int readInt() throws IOException;

    /**
     * 读取长整型。
     *
     * @return 长整型值。
     * @throws IOException IO异常
     */
    long readLong() throws IOException;

    /**
     * 读取浮点型。
     *
     * @return 浮点型值。
     * @throws IOException IO异常
     */
    float readFloat() throws IOException;

    /**
     * 读取双精度浮点型。
     *
     * @return 双精度浮点型值。
     * @throws IOException IO异常
     */
    double readDouble() throws IOException;

    /**
     * 读取 UTF-8 字符串。
     *
     * @return 字符串。
     * @throws IOException IO异常
     */
    String readUTF() throws IOException;

    /**
     * 读取字节数组。
     *
     * @return 字节数组。
     * @throws IOException IO异常
     */
    byte[] readBytes() throws IOException;
}