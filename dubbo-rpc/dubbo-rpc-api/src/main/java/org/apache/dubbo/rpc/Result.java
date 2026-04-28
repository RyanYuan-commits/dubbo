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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.Experimental;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;


/**
 * 一次调用的返回值，是 {@link Invoker#invoke(Invocation)} 方法的返回值
 * (API, Prototype, NonThreadSafe)
 * <p>
 * 目前的具体实现有：
 * <ol>
 *     <li> {@link AsyncRpcResult}：它是一个 {@link CompletionStage}，其底层值表示一次 RPC 调用的返回结果。 </li>
 *     <li> {@link AppResponse}：它虽然不可避免地继承了 {@link CompletionStage} 和 {@link Future}，
 *          但你绝不应该把 AppResponse 当作一种 Future 异步对象来使用，相反，它只是一个普通的具体类型。 </li>
 * </ol>
 *
 * @serial Don't change the class name and package name.
 * @see org.apache.dubbo.rpc.Invoker#invoke(Invocation)
 * @see AppResponse
 */
public interface Result extends Serializable {

    /**
     * 获取此次调用的返回值
     *
     * @return result. if no result return null.
     */
    Object getValue();

    /**
     * 设置此次调用的返回值
     *
     * @param value result. if no result return null.
     */
    void setValue(Object value);

    /**
     * 获取本次调用的异常
     *
     * @return exception. if no exception return null.
     */
    Throwable getException();

    /**
     * 设置此次调用的异常
     *
     * @param t exception
     */
    void setException(Throwable t);

    /**
     * 判断是否有异常
     *
     * @return has exception.
     */
    boolean hasException();

    /**
     * 复合操作，如果本次调用有异常，直接抛出异常，如果没有，获取结果
     *
     * @return result.
     * @throws if has exception throw it.
     */
    Object recreate() throws Throwable;

    Map<String, String> getAttachments();

    @Experimental("Experiment api for supporting Object transmission")
    Map<String, Object> getObjectAttachments();

    void addAttachments(Map<String, String> map);

    @Experimental("Experiment api for supporting Object transmission")
    void addObjectAttachments(Map<String, Object> map);

    /**
     * 使用 map 替换现有的 attachments
     */
    void setAttachments(Map<String, String> map);

    /**
     * 使用 map 替换现有的 attachments
     */
    @Experimental("Experiment api for supporting Object transmission")
    void setObjectAttachments(Map<String, Object> map);

    String getAttachment(String key);

    @Experimental("Experiment api for supporting Object transmission")
    Object getObjectAttachment(String key);

    String getAttachment(String key, String defaultValue);

    @Experimental("Experiment api for supporting Object transmission")
    Object getObjectAttachment(String key, Object defaultValue);

    void setAttachment(String key, String value);

    @Experimental("Experiment api for supporting Object transmission")
    void setAttachment(String key, Object value);

    @Experimental("Experiment api for supporting Object transmission")
    void setObjectAttachment(String key, Object value);

    /**
     * 添加一个回调，当RPC调用完成时，会触发这里添加的回调。
     * <p>
     * 正如方法名所暗示的，该方法将确保回调函数在与发起调用时完全相同的上下文环境中被触发。
     * 具体实现请参见 {@link Result#whenCompleteWithContext(BiConsumer)}。
     */
    Result whenCompleteWithContext(BiConsumer<Result, Throwable> fn);

    <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn);

    Result get() throws InterruptedException, ExecutionException;

    Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
}