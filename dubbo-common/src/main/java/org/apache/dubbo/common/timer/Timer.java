/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.dubbo.common.timer;

import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 定义了定时器的基本方法，执行的任务被定义为 {@link TimerTask}
 */
public interface Timer {

    /**
     * 在指定延迟后调度指定的 {@link TimerTask} 执行一次。
     *
     * @return 与指定任务关联的句柄
     * @throws IllegalStateException      如果此定时器已被 {@linkplain #stop() 停止}
     * @throws RejectedExecutionException 如果待处理的超时任务过多，创建新的超时任务可能导致系统不稳定
     */
    Timeout newTimeout(TimerTask task, long delay, TimeUnit unit);

    /**
     * 释放此 {@link Timer} 占用的所有资源，并取消所有已调度但尚未执行的任务。
     *
     * @return 被此方法取消的任务所关联的句柄集合
     */
    Set<Timeout> stop();

    /**
     * 判断定时器是否已停止。
     *
     * @return 如果已停止返回 true，否则返回 false
     */
    boolean isStop();

}