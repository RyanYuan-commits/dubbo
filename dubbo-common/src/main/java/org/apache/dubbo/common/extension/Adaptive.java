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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.ChannelHandler;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标识适配器逻辑
 * <p>
 * 当使用在类上时，表明当前类是一个适配器类，且内部自主实现了适配器逻辑
 * 如 {@link org.apache.dubbo.common.extension.factory.AdaptiveExtensionFactory}
 * </p>
 *
 * <p>
 * 当使用在方法上时，Dubbo 框架会介入，在运行时根据 URL 参数动态选择具体的拓展实现。
 * 如 {@link org.apache.dubbo.remoting.Transporter#bind(URL, ChannelHandler)}
 * </p>
 * <p>
 * 某个接口只能任选一种实现适配的方式
 * <ol>
 * <li>某个子类上标注 @Adaptive 注解，在子类内部实现适配器类</li>
 * <li>在当前接口的方法上标注 @Adaptive，指定 URL 参数（可选））</li>
 * </ol>
 * </p>
 *
 * @see ExtensionLoader
 * @see URL
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Adaptive {

    /**
     * 指定使用 URL 中的哪个参数作为注入的拓展名称。
     * <p>
     * 如果从 {@link URL} 中找不到指定的参数，则使用在 {@link SPI} 指定的默认扩展进行依赖注入
     * <p>
     * 例如，给定 <code>String[] {"key1", "key2"}</code>：
     * <ol>
     * <li>在 URL 中查找参数 'key1'，使用其值作为扩展的名称</li>
     * <li>如果在 URL 中找不到 'key1'（或其值为空），则尝试使用 'key2' 作为扩展名称</li>
     * <li>如果 'key2' 也不存在，则使用默认扩展</li>
     * <li>否则，抛出 {@link IllegalStateException}</li>
     * </ol>
     * 如果参数名称为空，则会根据接口的类名生成一个默认参数名，规则是：
     * 将类名按大写字母分成几部分，然后用点'.'分隔这些部分，例如，
     * 对于 {@code org.apache.dubbo.xxx.YyyInvokerWrapper}，生成的名称是
     * <code>String[] {"yyy.invoker.wrapper"}</code>。
     *
     * @return URL 中的参数名称
     */
    String[] value() default {};

}