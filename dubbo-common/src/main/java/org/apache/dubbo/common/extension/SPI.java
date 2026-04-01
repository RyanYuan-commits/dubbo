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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 扩展接口的标识注解
 * <p/>
 * 扩展配置文件格式的变更说明：<br/>
 * 以 Protocol 为例，其配置文件 'META-INF/dubbo/com.xxx.Protocol' 的格式从：<br/>
 * <pre>
 *     com.foo.XxxProtocol
 *     com.foo.YyyProtocol
 * </pre>
 * <p>
 * 变更为键值对格式：<br/>
 * <pre>
 *     xxx=com.foo.XxxProtocol
 *     yyy=com.foo.YyyProtocol
 * </pre>
 * <br/>
 * 格式变更的原因：
 * <p>
 * 如果扩展实现类中的静态字段或方法引用了第三方库，当该第三方库不存在时，扩展实现类将无法成功初始化。
 * 在这种情况下，如果沿用之前的文件格式，Dubbo 将无法识别扩展的标识 (id)，
 * 因此无法将异常信息与具体的扩展关联起来。
 * <p/>
 * 示例场景：
 * <p>
 * 加载 Extension("mina") 失败。当用户配置使用 mina 时，
 * 采用新格式后 Dubbo 能够明确指出是哪个扩展无法加载，
 * 而不是模糊地报告扩展加载失败，从而能提供更精确的错误信息和原因定位。
 * </p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface SPI {

    /**
     * 默认拓展名
     */
    String value() default "";

}