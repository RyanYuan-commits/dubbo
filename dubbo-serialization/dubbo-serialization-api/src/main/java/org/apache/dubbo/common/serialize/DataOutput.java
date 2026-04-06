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
 * 基本数据类型输出接口。
 */
public interface DataOutput {

    /**
     * 写入布尔值。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeBool(boolean v) throws IOException;

    /**
     * 写入字节。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeByte(byte v) throws IOException;

    /**
     * 写入短整型。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeShort(short v) throws IOException;

    /**
     * 写入整型。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeInt(int v) throws IOException;

    /**
     * 写入长整型。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeLong(long v) throws IOException;

    /**
     * 写入浮点型。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeFloat(float v) throws IOException;

    /**
     * 写入双精度浮点型。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeDouble(double v) throws IOException;

    /**
     * 写入字符串。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeUTF(String v) throws IOException;

    /**
     * 写入字节数组。
     *
     * @param v 值。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeBytes(byte[] v) throws IOException;

    /**
     * 写入字节数组。
     *
     * @param v 值。
     * @param off 数据中的起始偏移量。
     * @param len 要写入的字节数。
     * @throws IOException 如果发生 I/O 错误。
     */
    void writeBytes(byte[] v, int off, int len) throws IOException;

    /**
     * 刷新缓冲区。
     *
     * @throws IOException 如果发生 I/O 错误。
     */
    void flushBuffer() throws IOException;
}