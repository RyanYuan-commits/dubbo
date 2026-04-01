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
package org.apache.dubbo.common.extension;

/**
 * 拓展注入器，自适应拓展为 {@link org.apache.dubbo.common.extension.factory.AdaptiveExtensionFactory}
 * 在 {@link ExtensionLoader#injectExtension(Object) 为拓展类注入属性的方法} 中用于获取被注入的实例
 */
@SPI
public interface ExtensionFactory {

    /**
     * 通过类型和名称获取拓展属性
     *
     * @param type 拓展类型
     * @param name 拓展名称
     * @return 拓展实例
     */
    <T> T getExtension(Class<T> type, String name);

}
