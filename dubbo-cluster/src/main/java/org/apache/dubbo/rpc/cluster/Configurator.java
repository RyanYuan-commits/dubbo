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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.PRIORITY_KEY;

/**
 * Configurator. (SPI, Prototype, ThreadSafe)
 */
public interface Configurator extends Comparable<Configurator> {

    /**
     * 获取该 Configurator 对象对应的配置URL
     *
     * @return configurator url.
     */
    URL getUrl();

    /**
     * 返回经过 Configurator 修改后的URL
     *
     * @param url - old provider url.
     * @return new provider url.
     */
    URL configure(URL url);


    /**
     * 将多个配置 URL 对象解析成相应的 Configurator 对象
     * 将 override urls 转化为 map 结构，以便在重新引用时使用，每次都会推送全部规则，这些 URL 会被重新组装并计算
     * URL 协议：
     * <ol>
     * <li>override://0.0.0.0/...( or override://ip:port...?anyhost=true)&para1=value1... means global rules
     * (all of the providers take effect)</li>
     * <li>override://ip:port...?anyhost=false Special rules (only for a certain provider)</li>
     * <li>override:// rule is not supported... ,needs to be calculated by registry itself</li>
     * <li>override://0.0.0.0/ without parameters means clearing the override</li>
     * </ol>
     *
     * @param urls URL list to convert
     * @return converted configurator list
     */
    static Optional<List<Configurator>> toConfigurators(List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return Optional.empty();
        }

        // 创建 ConfiguratorFactory 适配器
        ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                .getAdaptiveExtension();

        List<Configurator> configurators = new ArrayList<>(urls.size());
        for (URL url : urls) {
            if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
                // 遇到 empty 协议，直接清空 configurators 集合，结束解析，返回空集合
                configurators.clear();
                break;
            }

            Map<String, String> override = new HashMap<>(url.getParameters());
            override.remove(ANYHOST_KEY);
            if (CollectionUtils.isEmptyMap(override)) {
                // 该配置没有携带任何参数，跳过
                continue;
            }

            // 创建对应的 Configurator 实例
            configurators.add(configuratorFactory.getConfigurator(url));
        }

        // 排序
        Collections.sort(configurators);
        return Optional.of(configurators);
    }

    /**
     * 首先按照 ip 进行排序，所有 ip 的优先级都高于 0.0.0.0；
     * 当ip相同时，按照 priority 值进行排序；
     */
    @Override
    default int compareTo(Configurator o) {
        if (o == null) {
            return -1;
        }

        int ipCompare = getUrl().getHost().compareTo(o.getUrl().getHost());
        // host is the same, sort by priority
        if (ipCompare == 0) {
            int i = getUrl().getParameter(PRIORITY_KEY, 0);
            int j = o.getUrl().getParameter(PRIORITY_KEY, 0);
            return Integer.compare(i, j);
        } else {
            return ipCompare;
        }
    }

}
