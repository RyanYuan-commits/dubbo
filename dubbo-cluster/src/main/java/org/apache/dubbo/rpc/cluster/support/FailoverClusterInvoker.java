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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RETRIES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;

/**
 * When invoke fails, log the initial error and retry other invokers (retry n times, which means at most n different invokers will be invoked)
 * Note that retry causes latency.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 *
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        List<Invoker<T>> copyInvokers = invokers;
        // 检查 invoker 是否为空，为空则会抛出异常
        checkInvokers(copyInvokers, invocation);
        String methodName = RpcUtils.getMethodName(invocation);
        // 获取重试次数，默认重试 2 次，循环执行 3 次
        int len = getUrl().getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1;
        if (len <= 0) {
            len = 1;
        }

        // 最后一次的异常
        RpcException le = null;
        // 记录已经尝试过的 invoker
        List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyInvokers.size());
        Set<String> providers = new HashSet<String>(len);
        for (int i = 0; i < len; i++) {
            // i > 1 表示触发重试，需要重新获取最新的服务列表
            if (i > 0) {
                checkWhetherDestroyed();
                // 重新调用 Directory.list() 方法，获取 Invoker 列表
                copyInvokers = list(invocation);
                // 检查 copyInvokers 是否为空，为空则抛出异常
                checkInvokers(copyInvokers, invocation);
            }

            // 通过 LoadBalance 选择 Invoker 对象，这里传入的 invoked 集合，
            // 是 AbstractClusterInvoker.select() 方法中的 selected 参数
            Invoker<T> invoker = select(loadbalance, invocation, copyInvokers, invoked);
            invoked.add(invoker);
            RpcContext.getContext().setInvokers((List) invoked);
            try {
                // 完成远程调用
                Result result = invoker.invoke(invocation);
                // 经过尝试之后成功，记录一个日志
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn("Although retry the method " + methodName
                            + " in the service " + getInterface().getName()
                            + " was successful by the provider " + invoker.getUrl().getAddress()
                            + ", but there have been failed providers " + providers
                            + " (" + providers.size() + "/" + copyInvokers.size()
                            + ") from the registry " + directory.getUrl().getAddress()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                            + le.getMessage(), le);
                }
                return result;
            } catch (RpcException e) {
                if (e.isBiz()) {
                    throw e;
                }
                le = e;
            } catch (Throwable e) {
                // 记录异常，表示此次调用失败，需要重试
                le = new RpcException(e.getMessage(), e);
            } finally {
                // 记录尝试过的 Provider 地址，会在日志中打印
                providers.add(invoker.getUrl().getAddress());
            }
        }

        // 达到重试上限，抛出异常，携带调用的方法名，尝试过的 Provider 节点地址等
        throw new RpcException(le.getCode(), "Failed to invoke the method "
                + methodName + " in the service " + getInterface().getName()
                + ". Tried " + len + " times of the providers " + providers
                + " (" + providers.size() + "/" + copyInvokers.size()
                + ") from the registry " + directory.getUrl().getAddress()
                + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                + Version.getVersion() + ". Last error is: "
                + le.getMessage(), le.getCause() != null ? le.getCause() : le);
    }

}
