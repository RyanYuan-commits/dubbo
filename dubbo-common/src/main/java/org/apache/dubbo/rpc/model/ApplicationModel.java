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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.context.FrameworkExt;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.context.ConfigManager;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * 当前 {@link ExtensionLoader}, {@code DubboBootstrap} 以及当前类
 * 被设计成单例或静态的 (整体是静态的或者包含静态属性). 所以它们的实力具有类加载器或进程作用域,
 * 如果你希望在单个进程中包含多个 Dubbo Server, 你需要重构上面提到的三个类
 * <p>
 * 表示一个使用 Dubbo 的应用, 并存储在 RPC 调用过程中需要的元数据
 * <p>
 * ApplicationModel 包含许多 ProviderModel (关于已发布的服务) 和
 * 许多 ConsumerModel (关于已订阅的服务)
 * <p>
 */
public class ApplicationModel {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ApplicationModel.class);
    public static final String NAME = "application";

    private static AtomicBoolean INIT_FLAG = new AtomicBoolean(false);

    public static void init() {
        if (INIT_FLAG.compareAndSet(false, true)) {
            ExtensionLoader<ApplicationInitListener> extensionLoader = ExtensionLoader.getExtensionLoader(ApplicationInitListener.class);
            Set<String> listenerNames = extensionLoader.getSupportedExtensions();
            for (String listenerName : listenerNames) {
                extensionLoader.getExtension(listenerName).init();
            }
        }
    }

    public static Collection<ConsumerModel> allConsumerModels() {
        return getServiceRepository().getReferredServices();
    }

    public static Collection<ProviderModel> allProviderModels() {
        return getServiceRepository().getExportedServices();
    }

    public static ProviderModel getProviderModel(String serviceKey) {
        return getServiceRepository().lookupExportedService(serviceKey);
    }

    public static ConsumerModel getConsumerModel(String serviceKey) {
        return getServiceRepository().lookupReferredService(serviceKey);
    }

    private static final ExtensionLoader<FrameworkExt> LOADER = ExtensionLoader.getExtensionLoader(FrameworkExt.class);

    public static void initFrameworkExts() {
        Set<FrameworkExt> exts = ExtensionLoader.getExtensionLoader(FrameworkExt.class).getSupportedExtensionInstances();
        for (FrameworkExt ext : exts) {
            ext.initialize();
        }
    }

    public static Environment getEnvironment() {
        return (Environment) LOADER.getExtension(Environment.NAME);
    }

    public static ConfigManager getConfigManager() {
        return (ConfigManager) LOADER.getExtension(ConfigManager.NAME);
    }

    public static ServiceRepository getServiceRepository() {
        return (ServiceRepository) LOADER.getExtension(ServiceRepository.NAME);
    }

    public static ApplicationConfig getApplicationConfig() {
        return getConfigManager().getApplicationOrElseThrow();
    }

    public static String getName() {
        return getApplicationConfig().getName();
    }

    @Deprecated
    private static String application;

    @Deprecated
    public static String getApplication() {
        return application == null ? getName() : application;
    }

    // Currently used by UT.
    @Deprecated
    public static void setApplication(String application) {
        ApplicationModel.application = application;
    }

    // only for unit test
    public static void reset() {
        getServiceRepository().destroy();
        getConfigManager().destroy();
        getEnvironment().destroy();
    }

}
