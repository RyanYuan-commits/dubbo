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
package org.apache.dubbo.common.threadpool;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 这个 Executor 没有管理任何线程，而是将提交的任务存放到阻塞队列中，一个线程可以调用 {@link #waitAndDrain()} 方法阻塞，如果有任务抵达，
 * 这个线程会被唤醒并执行此任务，在 AbstractInvoker#getCallbackExecutor 中创建。
 */
public class ThreadlessExecutor extends AbstractExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(ThreadlessExecutor.class.getName());

    /**
     * 阻塞队列，用于在 I/O 线程和业务线程之间传递任务
     */
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

    private ExecutorService sharedExecutor;

    /**
     * 指向请求对应的 {@link org.apache.dubbo.remoting.exchange.support.DefaultFuture DefaultFuture} 对象
     */
    private CompletableFuture<?> waitingFuture;

    /**
     * @see #waitAndDrain()
     */
    private boolean finished = false;
    private volatile boolean waiting = true;

    private final Object lock = new Object();

    public ThreadlessExecutor(ExecutorService sharedExecutor) {
        this.sharedExecutor = sharedExecutor;
    }

    public CompletableFuture<?> getWaitingFuture() {
        return waitingFuture;
    }

    public void setWaitingFuture(CompletableFuture<?> waitingFuture) {
        this.waitingFuture = waitingFuture;
    }

    public boolean isWaiting() {
        return waiting;
    }

    /**
     * 等待直到获取到任务，这个任务可能是正常响应或者超时响应。
     * <p>
     * 通常情况下，{@link #waitAndDrain()} 只会被调用一次，一旦响应返回，{@link #waitAndDrain()} 就会返回；
     * 之后对 {@link #waitAndDrain()} 的调用应当立刻返回。
     * 无需担心 {@link #finished} 的线程安全性，因为对它的检查和更新都在 {@link  #waitAndDrain()} 方法中进行，
     * 且该方法与一次 RPC 调用（单个线程）绑定，因此其调用是完全顺序性的。
     */
    public void waitAndDrain() throws InterruptedException {
        if (finished) {
            return;
        }

        Runnable runnable = queue.take();

        synchronized (lock) {
            waiting = false;
            runnable.run();
        }

        runnable = queue.poll();
        while (runnable != null) {
            try {
                runnable.run();
            } catch (Throwable t) {
                logger.info(t);

            }
            runnable = queue.poll();
        }
        // mark the status of ThreadlessExecutor as finished.
        finished = true;
    }

    public long waitAndDrain(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        /*long startInMs = System.currentTimeMillis();
        Runnable runnable = queue.poll(timeout, unit);
        if (runnable == null) {
            throw new TimeoutException();
        }
        runnable.run();
        long elapsedInMs = System.currentTimeMillis() - startInMs;
        long timeLeft = timeout - elapsedInMs;
        if (timeLeft < 0) {
            throw new TimeoutException();
        }
        return timeLeft;*/
        throw new UnsupportedOperationException();
    }

    /**
     * If the calling thread is still waiting for a callback task, add the task into the blocking queue to wait for schedule.
     * Otherwise, submit to shared callback executor directly.
     *
     * @param runnable
     */
    @Override
    public void execute(Runnable runnable) {
        synchronized (lock) {
            if (!waiting) {
                sharedExecutor.execute(runnable);
            } else {
                queue.add(runnable);
            }
        }
    }

    /**
     * tells the thread blocking on {@link #waitAndDrain()} to return, despite of the current status, to avoid endless waiting.
     */
    public void notifyReturn(Throwable t) {
        // an empty runnable task.
        execute(() -> {
            waitingFuture.completeExceptionally(t);
        });
    }

    /**
     * The following methods are still not supported
     */
    @Override
    public void shutdown() {
        shutdownNow();
    }

    @Override
    public List<Runnable> shutdownNow() {
        notifyReturn(new IllegalStateException("Consumer is shutting down and this call is going to be stopped without " +
                "receiving any result, usually this is called by a slow provider instance or bad service implementation."));
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }
}
