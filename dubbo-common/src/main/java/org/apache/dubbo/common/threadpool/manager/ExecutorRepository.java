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
package org.apache.dubbo.common.threadpool.manager;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.SPI;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 负责管理 Dubbo 中的线程池，目前仅有一个实现 {@link DefaultExecutorRepository}
 */
@SPI("default")
public interface ExecutorRepository {

    /**
     * 当 Client 或 Server 启动时，调用该方法，根据 URL 生成一个线程池
     */
    ExecutorService createExecutorIfAbsent(URL url);

    ExecutorService getExecutor(URL url);

    /**
     * 根据 URL 修改线程池的部分属性，如 coreSize、mixSize 等。
     */
    void updateThreadpool(URL url, ExecutorService executor);

    ScheduledExecutorService nextScheduledExecutor();

    ScheduledExecutorService getServiceExporterExecutor();

    /**
     * 获取默认的，全局共享的线程池
     */
    ExecutorService getSharedExecutor();

}
