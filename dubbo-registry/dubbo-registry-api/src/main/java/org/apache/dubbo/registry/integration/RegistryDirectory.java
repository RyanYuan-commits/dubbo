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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.AddressListener;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.governance.GovernanceRuleRepository;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.DISABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.APP_DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.COMPATIBLE_CONFIG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTE_PROTOCOL;
import static org.apache.dubbo.registry.Constants.CONFIGURATORS_SUFFIX;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.SIMPLIFIED_KEY;
import static org.apache.dubbo.registry.integration.RegistryProtocol.DEFAULT_REGISTER_CONSUMER_KEYS;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;


/**
 * RegistryDirectory，监听 providers、routers、configurators 三个目录
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    /**
     * 集群策略适配器
     */
    private static final Cluster CLUSTER = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    /**
     * 路由工厂适配器
     */
    private static final RouterFactory ROUTER_FACTORY = ExtensionLoader.getExtensionLoader(RouterFactory.class)
            .getAdaptiveExtension();

    /**
     * {interface}:{group}:{version}
     */
    private final String serviceKey;

    /**
     * 服务接口雷ixng
     */
    private final Class<T> serviceType;

    /**
     * Consumer URL 中 refer 参数解析后得到的全部 kv
     */
    private final Map<String, String> queryMap;

    /**
     * 只保留 Consumer 属性的 URL，也就是由 queryMap 集合重新生成的 URL
     */
    private final URL directoryUrl;

    /**
     * 是否引用多个服务组
     */
    private final boolean multiGroup;

    /**
     * 使用的 Protocol 实现
     */
    private Protocol protocol;

    /**
     * 使用的注册中心实现
     */
    private Registry registry;
    private volatile boolean forbidden = false;
    private boolean shouldRegister;
    private boolean shouldSimplified;

    private volatile URL overrideDirectoryUrl;

    private volatile URL registeredConsumerUrl;

    /**
     * 动态更新的配置信息
     * override rules
     * Priority: override > -D > consumer > provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    private volatile List<Configurator> configurators; 

    /**
     * <provider_url:invoker> 随着 invoker 动态更新
     */
    private volatile Map<String, Invoker<T>> urlInvokerMap; 

    /**
     * 动态更新得 invoker 集合
     */
    private volatile List<Invoker<T>> invokers;

    /**
     * 当前缓存的所有 Provider 的 URL，该集合会与 invokers 字段同时动态更新
     */
    private volatile Set<URL> cachedInvokerUrls; 

    private static final ConsumerConfigurationListener CONSUMER_CONFIGURATION_LISTENER = new ConsumerConfigurationListener();
    private ReferenceConfigurationListener serviceConfigurationListener;

    /**
     * 根据传入的注册中心 URL 初始化核心字段
     *
     * @param serviceType service clazz
     * @param url 注册中心的URL，如 zookeeper://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?...，
     *            其中 refer 参数包含 Consumer 信息，如 refer=application=dubbo-demo-api-consumer&dubbo=2.0.2&interface=
     *            org.apache.dubbo.demo.DemoService&pid=13423&register.ip=192.168.124.3&side=consumer
     */
    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }

        shouldRegister = !ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(REGISTER_KEY, true);
        shouldSimplified = url.getParameter(SIMPLIFIED_KEY, false);
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        // 解析 refer 参数值，得到其中 Consumer 的属性信息
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(REFER_KEY));
        // 根据 queryMap 中的 KV 重新构造 URL，其中的 protocol 和 path 部分不变
        this.overrideDirectoryUrl = this.directoryUrl = turnRegistryUrlToConsumerUrl(url);
        String group = directoryUrl.getParameter(GROUP_KEY, "");
        this.multiGroup = group != null && (ANY_VALUE.equals(group) || group.contains(","));
    }

    private URL turnRegistryUrlToConsumerUrl(URL url) {
        return URLBuilder.from(url)
                .setPath(url.getServiceInterface())
                .clearParameters()
                .addParameters(queryMap)
                .removeParameter(MONITOR_KEY)
                .build();
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public Registry getRegistry() {
        return registry;
    }

    public boolean isShouldRegister() {
        return shouldRegister;
    }

    public void subscribe(URL url) {
        setConsumerUrl(url);
        // 将当前 RegistryDirectory 对象作为 ConfigurationListener 记录到 CONSUMER_CONFIGURATION_LISTENER 中
        CONSUMER_CONFIGURATION_LISTENER.addNotifyListener(this);
        serviceConfigurationListener = new ReferenceConfigurationListener(this, url);
        // 完成订阅操作
        registry.subscribe(url, this);
    }

    public void unSubscribe(URL url) {
        setConsumerUrl(null);
        CONSUMER_CONFIGURATION_LISTENER.removeNotifyListener(this);
        serviceConfigurationListener.stop();
        registry.unsubscribe(url, this);
    }


    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }

        // unregister.
        try {
            if (getRegisteredConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unregister(getRegisteredConsumerUrl());
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unregister service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        // unsubscribe.
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
            ExtensionLoader.getExtensionLoader(GovernanceRuleRepository.class).getDefaultExtension()
                    .removeListener(ApplicationModel.getApplication(), CONSUMER_CONFIGURATION_LISTENER);
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // must be executed after unsubscribing
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    @Override
    public synchronized void notify(List<URL> urls) {
        // 按照 category 进行分类，分成 configurators、routers、providers 三类
        Map<String, List<URL>> categoryUrls = urls.stream()
                .filter(Objects::nonNull)
                .filter(this::isValidCategory)
                .filter(this::isNotCompatibleFor26x)
                .collect(Collectors.groupingBy(this::judgeCategory));

        // 获取 configurators 类型的URL，并转换成 Configurator 对象
        List<URL> configuratorURLs = categoryUrls.getOrDefault(CONFIGURATORS_CATEGORY, Collections.emptyList());
        this.configurators = Configurator.toConfigurators(configuratorURLs).orElse(this.configurators);

        // 获取 routers 类型的 URL，并转成 Router 对象，添加到 RouterChain 中
        List<URL> routerURLs = categoryUrls.getOrDefault(ROUTERS_CATEGORY, Collections.emptyList());
        toRouters(routerURLs).ifPresent(this::addRouters);

        // 获取 providers 类型的 URL，调用 refreshOverrideAndInvoker() 方法进行处理
        List<URL> providerURLs = categoryUrls.getOrDefault(PROVIDERS_CATEGORY, Collections.emptyList());
        /*
         * 在 Dubbo3.0 中会触发 AddressListener 监听器，但是现在 AddressListener 接口还没有实现
         */
        ExtensionLoader<AddressListener> addressListenerExtensionLoader = ExtensionLoader.getExtensionLoader(AddressListener.class);
        List<AddressListener> supportedListeners = addressListenerExtensionLoader.getActivateExtension(getUrl(), (String[]) null);
        if (supportedListeners != null && !supportedListeners.isEmpty()) {
            for (AddressListener addressListener : supportedListeners) {
                providerURLs = addressListener.notify(providerURLs, getConsumerUrl(),this);
            }
        }
        refreshOverrideAndInvoker(providerURLs);
    }

    private String judgeCategory(URL url) {
        if (UrlUtils.isConfigurator(url)) {
            return CONFIGURATORS_CATEGORY;
        } else if (UrlUtils.isRoute(url)) {
            return ROUTERS_CATEGORY;
        } else if (UrlUtils.isProvider(url)) {
            return PROVIDERS_CATEGORY;
        }
        return "";
    }

    private void refreshOverrideAndInvoker(List<URL> urls) {
        // mock zookeeper://xxx?mock=return null
        overrideDirectoryUrl();
        refreshInvoker(urls);
    }

    /**
     * 将调用者 URL 列表转换为调用者（Invoker）映射集合。转换规则如下：
     * <ol>
     * <li> 如果 URL 已经被转换为调用者，则不再重新引用，直接从缓存中获取；
     * 注意：URL 中任意参数发生变化，都需要重新进行引用。</li>
     * <li> 如果传入的调用者 URL 列表不为空，则表示这是最新的调用者 URL 列表。</li>
     * <li> 如果传入的调用者 URL 列表为空，则表示当前仅存在覆盖规则或路由规则，
     * 需要重新比对，以决定是否需要重新引用。</li>
     * </ol>
     *
     * @param invokerUrls 该参数禁止为 null
     */
    // TODO: 2017/8/31 FIXME The thread pool should be used to refresh the address, otherwise the task may be accumulated.
    private void refreshInvoker(List<URL> invokerUrls) {
        Assert.notNull(invokerUrls, "invokerUrls should not be null");

        if (invokerUrls.size() == 1
                && invokerUrls.get(0) != null
                && EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            // invokerUrls 集合部位空，长度为 1，并且协议为 empty，表示所有服务都下线的情况
            // 销毁所有 Provider 对应的 Invoker
            // forbidden 标记为 true，后续所有请求直接抛异常
            this.forbidden = true;
            this.invokers = Collections.emptyList();
            // 清空 RouterChain 中的 invoker 集合
            routerChain.setInvokers(this.invokers);
            // 关闭所有的 invoker
            destroyAllInvokers();
        } else {
            // 表示可以正常处理数据请求
            this.forbidden = false;
            // <provider_key:url>
            Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap;
            if (invokerUrls == Collections.<URL>emptyList()) {
                invokerUrls = new ArrayList<>();
            }
            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
                // providers 目录未发生变化，使用 cachedInvokerUrls 作为最新的值
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                // 使用 invokerUrls 更新 cachedInvokerUrls 的值
                this.cachedInvokerUrls = new HashSet<>();
                this.cachedInvokerUrls.addAll(invokerUrls);
            }
            if (invokerUrls.isEmpty()) {
                // providers 目录未发生变更，无须处理
                return;
            }

            // 将 invokerUrls 转化为对应的 Invoker 映射关系
            Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);

            /*
             * 如果计算/校验结果异常，则不进行处理。
             *
             * 1. 客户端配置的协议与服务端协议不一致。例如：消费者协议为 dubbo，而提供者仅提供 rest 等其他协议的服务。
             * 2. 注册中心不稳定，推送了不符合规范的非法数据。
             */
            if (CollectionUtils.isEmptyMap(newUrlInvokerMap)) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls
                        .toString()));
                return;
            }

            // 更新 invokers 字段和 urlInvokerMap 集合
            List<Invoker<T>> newInvokers = Collections.unmodifiableList(new ArrayList<>(newUrlInvokerMap.values()));
            // 预先路由并构建缓存，注意：路由缓存必须基于原始的 Invoker 列表构建。
            // toMergeMethodInvokerMap() 方法会对部分不同分组的 invoker 进行包装，这些被包装的 invoker 不应该参与路由。
            routerChain.setInvokers(newInvokers);
            // 针对 multiGroup 的特殊处理，将每个 group 中的 Invoker 合并为一个 Invoker
            this.invokers = multiGroup ? toMergeInvokerList(newInvokers) : newInvokers;
            this.urlInvokerMap = newUrlInvokerMap;

            try {
                // 比较新旧两组Invoker集合，销毁掉已经下线的Invoker
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap); // Close the unused Invoker
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
    }

    /**
     * 根据服务分组合并 Invoker，每个 group 合并为一个虚拟的 Invoker
     *
     * @param invokers invokers
     * @return merged invoker list
     */
    private List<Invoker<T>> toMergeInvokerList(List<Invoker<T>> invokers) {
        List<Invoker<T>> mergedInvokers = new ArrayList<>();
        Map<String, List<Invoker<T>>> groupMap = new HashMap<>();
        for (Invoker<T> invoker : invokers) {
            // 按照 group 分组 invoker
            String group = invoker.getUrl().getParameter(GROUP_KEY, "");
            groupMap.computeIfAbsent(group, k -> new ArrayList<>());
            groupMap.get(group).add(invoker);
        }

        if (groupMap.size() == 1) {
            // 如果只有一个 group，则直接使用该 group 分组对应的 invoker
            mergedInvokers.addAll(groupMap.values().iterator().next());
        } else if (groupMap.size() > 1) {
            // 将每个 group 对应的 Invoker 集合合并成一个 Invoker
            for (List<Invoker<T>> groupList : groupMap.values()) {
                // 使用 StaticDirectory 以及 Cluster 合并每个 group 中的 Invoker
                StaticDirectory<T> staticDirectory = new StaticDirectory<>(groupList);
                staticDirectory.buildRouterChain();
                mergedInvokers.add(CLUSTER.join(staticDirectory));
            }
        } else {
            mergedInvokers = invokers;
        }
        return mergedInvokers;
    }

    /**
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private Optional<List<Router>> toRouters(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Optional.empty();
        }

        List<Router> routers = new ArrayList<>();
        for (URL url : urls) {
            if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
                continue;
            }
            String routerType = url.getParameter(ROUTER_KEY);
            if (routerType != null && routerType.length() > 0) {
                url = url.setProtocol(routerType);
            }
            try {
                Router router = ROUTER_FACTORY.getRouter(url);
                if (!routers.contains(router)) {
                    routers.add(router);
                }
            } catch (Throwable t) {
                logger.error("convert router url to router error, url: " + url, t);
            }
        }

        return Optional.of(routers);
    }

    /**
     * Turn urls into invokers, and if url has been refer, will not re-reference.
     *
     * @param urls
     * @return invokers
     */
    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<>();
        if (urls == null || urls.isEmpty()) {
            return newUrlInvokerMap;
        }
        Set<String> keys = new HashSet<>();
        // 获取 Consumer 端支持的 protocols
        String queryProtocols = this.queryMap.get(PROTOCOL_KEY);
        for (URL providerUrl : urls) {
            // 遍历所有Consumer端支持的协议
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {
                    // 如果当前 URL 不支持 Consumer 端的协议，也就无法执行后续转换成 Invoker 的逻辑
                    continue;
                }
            }
            if (EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                // 跳过 empty 协议的 URL
                continue;
            }
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                // 如果 Consumer 端不支持该 URL 的协议，也会跳过该URL
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() +
                        " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() +
                        " to consumer " + NetUtils.getLocalHost() + ", supported protocol: " +
                        ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }
            // 合并 URL 参数
            URL url = mergeUrl(providerUrl);

            // 获取完整 URL 对应的字符串，也就是在 urlInvokerMap 集合中的 key
            String key = url.toFullString();
            if (keys.contains(key)) {
                // 跳过重复的 URL
                continue;
            }
            keys.add(key);

            // 查询 urlInvokerMap，检查该 Invoker 是否已经创建，已创建则复用，否则创建新的 Invoker
            Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);
            if (invoker == null) { // Not in the cache, refer again
                try {
                    boolean enabled = true;
                    if (url.hasParameter(DISABLED_KEY)) {
                        enabled = !url.getParameter(DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(ENABLED_KEY, true);
                    }
                    if (enabled) {
                        // 通过 Protocol.refer() 方法创建对应的 Invoker 对象
                        invoker = new InvokerDelegate<>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) {
                    // 将 key 和 Invoker 对象之间的映射关系记录到 newUrlInvokerMap 中
                    newUrlInvokerMap.put(key, invoker);
                }
            } else {
                // 缓存命中，直接将 urlInvokerMap 中的 Invoker 转移到 newUrlInvokerMap 即可
                newUrlInvokerMap.put(key, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * 将注册中心中 configurators 目录下的 URL（override 协议）以及辅助治理控制台动态添加的配置与 Provider 端的配置进行
     * 合并，覆盖 Provider URL 中的一些信息，优先级：override > -D >Consumer > Provider
     */
    private URL mergeUrl(URL providerUrl) {
        // 移除 Provider URL 中只在 Provider 端生效的属性，如 threadpool、threads、queues 等
        // 然后，用 Consumer 端的配置覆盖 Provider URL 的相应配置，其中，version、group、methods、timestamp 等参数以
        // Provider 端的配置优先
        // 最后，合并 Provider 端和 Consumer 端配置的 Filter 以及 Listener
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap);

        // 合并 configurators 类型的 URL，configurators 类型的 URL 又分为三类：
        // 第一类是注册中心 Configurators 目录下新增的 URL（override协议）
        // 第二类是通过 ConsumerConfigurationListener 监听器(监听应用级别的配置)得到的动态配置
        // 第三类是通过 ReferenceConfigurationListener 监听器(监听服务级别的配置)得到的动态配置
        // 除了注册中心的configurators目录下有配置信息之外，还有可以在服务治理控制台动态添加配置，
        // ConsumerConfigurationListener、ReferenceConfigurationListener监听器就是用来监听服务治理控制台的动态配置的
        providerUrl = overrideWithConfigurator(providerUrl);

        // 增加 check=false，即只有在调用时，才检查 Provider 是否可用
        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false));

        // 重新复制 overrideDirectoryUrl，providerUrl 在经过第一步参数合并后赋值给 overrideDirectoryUrl。
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters()); // Merge the provider side parameters

        // 对低版本 Dubbo 的兼容逻辑
        if ((providerUrl.getPath() == null || providerUrl.getPath()
                .length() == 0) && DUBBO_PROTOCOL.equals(providerUrl.getProtocol())) { // Compatible version 1.0
            //fix by tony.chenl DUBBO-44
            String path = directoryUrl.getParameter(INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    private URL overrideWithConfigurator(URL providerUrl) {
        // override url with configurator from "override://" URL for dubbo 2.6 and before
        providerUrl = overrideWithConfigurators(this.configurators, providerUrl);

        // override url with configurator from configurator from "app-name.configurators"
        providerUrl = overrideWithConfigurators(CONSUMER_CONFIGURATION_LISTENER.getConfigurators(), providerUrl);

        // override url with configurator from configurators from "service-name.configurators"
        if (serviceConfigurationListener != null) {
            providerUrl = overrideWithConfigurators(serviceConfigurationListener.getConfigurators(), providerUrl);
        }

        return providerUrl;
    }

    private URL overrideWithConfigurators(List<Configurator> configurators, URL url) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
        }
        return url;
    }

    /**
     * Close all invokers
     */
    private void destroyAllInvokers() {
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        invokers = null;
    }

    /**
     * Check whether the invoker in the cache needs to be destroyed
     * If set attribute of url: refer.autodestroy=false, the invokers will only increase without decreasing,there may be a refer leak
     *
     * @param oldUrlInvokerMap
     * @param newUrlInvokerMap
     */
    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
        }
        // check deleted invoker
        List<String> deleted = null;
        if (oldUrlInvokerMap != null) {
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
                if (!newInvokers.contains(entry.getValue())) {
                    if (deleted == null) {
                        deleted = new ArrayList<>();
                    }
                    deleted.add(entry.getKey());
                }
            }
        }

        if (deleted != null) {
            for (String url : deleted) {
                if (url != null) {
                    Invoker<T> invoker = oldUrlInvokerMap.remove(url);
                    if (invoker != null) {
                        try {
                            invoker.destroy();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] failed. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            // 检测 forbidden 字段，当该字段在 refreshInvoker() 过程中设置为 true 时，表示无 Provider 可用，直接抛出异常
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "No provider available from registry " +
                    getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +
                    NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() +
                    ", please check status of providers(disabled, not registered or in blacklist).");
        }

        if (multiGroup) {
            // multiGroup 为 true 时的特殊处理，在 refreshInvoker() 方法中针对 multiGroup 为 true 的场景，已经使用
            // Router 进行了筛选，所以这里直接返回接口
            return this.invokers == null ? Collections.emptyList() : this.invokers;
        }

        List<Invoker<T>> invokers = null;
        try {
            // 通过 RouterChain.route() 方法筛选 Invoker 集合，最终得到符合路由条件的 Invoker 集合
            invokers = routerChain.route(getConsumerUrl(), invocation);
        } catch (Throwable t) {
            logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
        }

        return invokers == null ? Collections.emptyList() : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public List<Invoker<T>> getAllInvokers() {
        return invokers;
    }

    @Override
    public URL getConsumerUrl() {
        return this.overrideDirectoryUrl;
    }

    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    public void setRegisteredConsumerUrl(URL url) {
        if (!shouldSimplified) {
            this.registeredConsumerUrl = url.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY,
                    String.valueOf(false));
        } else {
            this.registeredConsumerUrl = URL.valueOf(url, DEFAULT_REGISTER_CONSUMER_KEYS, null).addParameters(
                    CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false));
        }
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(url));
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    private boolean isValidCategory(URL url) {
        String category = url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
        if ((ROUTERS_CATEGORY.equals(category) || ROUTE_PROTOCOL.equals(url.getProtocol())) ||
                PROVIDERS_CATEGORY.equals(category) ||
                CONFIGURATORS_CATEGORY.equals(category) || DYNAMIC_CONFIGURATORS_CATEGORY.equals(category) ||
                APP_DYNAMIC_CONFIGURATORS_CATEGORY.equals(category)) {
            return true;
        }
        logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " +
                getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
        return false;
    }

    private boolean isNotCompatibleFor26x(URL url) {
        return StringUtils.isEmpty(url.getParameter(COMPATIBLE_CONFIG_KEY));
    }

    private void overrideDirectoryUrl() {
        // merge override parameters
        this.overrideDirectoryUrl = directoryUrl;
        List<Configurator> localConfigurators = this.configurators; // local reference
        doOverrideUrl(localConfigurators);
        List<Configurator> localAppDynamicConfigurators = CONSUMER_CONFIGURATION_LISTENER.getConfigurators(); // local reference
        doOverrideUrl(localAppDynamicConfigurators);
        if (serviceConfigurationListener != null) {
            List<Configurator> localDynamicConfigurators = serviceConfigurationListener.getConfigurators(); // local reference
            doOverrideUrl(localDynamicConfigurators);
        }
    }

    private void doOverrideUrl(List<Configurator> configurators) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (Configurator configurator : configurators) {
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
    }

    /**
     * The delegate class, which is mainly used to store the URL address sent by the registry,and can be reassembled on the basis of providerURL queryMap overrideMap for re-refer.
     *
     * @param <T>
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }

    private static class ReferenceConfigurationListener extends AbstractConfiguratorListener {

        private RegistryDirectory directory;

        private URL url;

        ReferenceConfigurationListener(RegistryDirectory directory, URL url) {
            this.directory = directory;
            this.url = url;
            this.initWith(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        void stop() {
            this.stopListen(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        @Override
        protected void notifyOverrides() {
            // 通知 configurator/router 变化
            directory.refreshOverrideAndInvoker(Collections.emptyList());
        }
    }

    private static class ConsumerConfigurationListener extends AbstractConfiguratorListener {
        List<RegistryDirectory> listeners = new ArrayList<>();

        ConsumerConfigurationListener() {
            this.initWith(ApplicationModel.getApplication() + CONFIGURATORS_SUFFIX);
        }

        void addNotifyListener(RegistryDirectory listener) {
            this.listeners.add(listener);
        }

        void removeNotifyListener(RegistryDirectory listener) {
            this.listeners.remove(listener);
        }

        @Override
        protected void notifyOverrides() {
            listeners.forEach(listener -> listener.refreshOverrideAndInvoker(Collections.emptyList()));
        }
    }

}
