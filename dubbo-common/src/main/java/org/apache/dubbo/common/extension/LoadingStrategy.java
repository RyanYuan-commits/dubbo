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

import org.apache.dubbo.common.lang.Prioritized;

public interface LoadingStrategy extends Prioritized {

    /**
     * 定义当前加载策略扫描的目录，如 META-INF/dubbo/internal/
     * @return 目录
     */
    String directory();

    default boolean preferExtensionClassLoader() {
        return false;
    }

    default String[] excludedPackages() {
        return null;
    }

    /**
     * 如果在加载过程中发现前面已经加载过同名拓展类，本次加载是否会覆盖前面的拓展类
     *
     * @return 如果是，return true；反之，return false
     * @since 2.7.7
     */
    default boolean overridden() {
        return false;
    }

}
