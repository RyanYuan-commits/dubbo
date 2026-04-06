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

/**
 * 为不同的序列化实现分配了唯一的字节类型标识符，这些 ID 在协议编码过程中被写入协议头，
 * 确保通信双方能够正确识别和解析数据格式，其最大值为 31。
 *
 * @see Serialization#getContentTypeId()
 */
public interface Constants {

    byte HESSIAN2_SERIALIZATION_ID = 2;
    byte JAVA_SERIALIZATION_ID = 3;
    byte COMPACTED_JAVA_SERIALIZATION_ID = 4;
    byte FASTJSON_SERIALIZATION_ID = 6;
    byte NATIVE_JAVA_SERIALIZATION_ID = 7;
    byte KRYO_SERIALIZATION_ID = 8;
    byte FST_SERIALIZATION_ID = 9;
    byte NATIVE_HESSIAN_SERIALIZATION_ID = 10;
    byte PROTOSTUFF_SERIALIZATION_ID = 12;
    byte AVRO_SERIALIZATION_ID = 11;
    byte GSON_SERIALIZATION_ID = 16;
    byte PROTOBUF_JSON_SERIALIZATION_ID = 21;

    byte PROTOBUF_SERIALIZATION_ID = 22;
    byte KRYO_SERIALIZATION2_ID = 25;

}
