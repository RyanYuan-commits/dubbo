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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 一种针对近似 I/O 超时调度优化的 {@link Timer}。
 *
 * <h3>刻度时长（Tick Duration）</h3>
 * <p>
 * 正如“近似”所述，此定时器不会精确地在预定时间执行 {@link TimerTask}。
 * {@link HashedWheelTimer} 会在每个刻度检查是否有落后的 {@link TimerTask} 并执行它们。
 * <p>
 * 您可以通过在构造函数中指定更小或更大的刻度时长来提高或降低执行时间的精度。
 * 在大多数网络应用中，I/O 超时不需要非常精确。因此，默认刻度时长为 100 毫秒，
 * 在大多数情况下您无需尝试不同的配置。
 *
 * <h3>每轮刻度数（轮大小）</h3>
 * <p>
 * {@link HashedWheelTimer} 维护一个称为“轮（wheel）”的数据结构。
 * 简单来说，轮是一个 {@link TimerTask} 的哈希表，根据任务的截止时间来做哈希运算。
 * 每轮的默认刻度数（即轮的大小）为 512。如果您计划调度大量超时任务，可以指定更大的值。
 *
 * <h3>不要创建多个实例</h3>
 * <p>
 * {@link HashedWheelTimer} 在每次实例化并启动时都会创建一个新线程。
 * 因此，您应确保只创建一个实例并在整个应用中共享它。
 * 常见的错误是为每个连接创建一个新实例，这会导致应用程序无响应。
 *
 * <h3>实现细节</h3>
 * <p>
 * {@link HashedWheelTimer} 基于
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> 和
 * Tony Lauck 的论文：
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>。更全面的幻灯片资料位于
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">此处</a>。
 */
public class HashedWheelTimer implements Timer {

    public static final String NAME = "hased";

    private static final Logger logger = LoggerFactory.getLogger(HashedWheelTimer.class);

    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private static final AtomicBoolean WARNED_TOO_MANY_INSTANCES = new AtomicBoolean();
    private static final int INSTANCE_COUNT_LIMIT = 64;
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");

    /**
     * 实现 Runnable 接口，用于封装工作逻辑
     */
    private final Worker worker = new Worker();

    /**
     * 工作线程
     */
    private final Thread workerThread;

    /**
     * @see HashedWheelTimer#workerState
     */
    private static final int WORKER_STATE_INIT = 0;
    private static final int WORKER_STATE_STARTED = 1;
    private static final int WORKER_STATE_SHUTDOWN = 2;

    /**
     * 工作线程的状态
     * <ol>
     * <li>{@link HashedWheelTimer#WORKER_STATE_INIT} 初始化</li>
     * <li>{@link HashedWheelTimer#WORKER_STATE_STARTED} 启动</li>
     * <li>{@link HashedWheelTimer#WORKER_STATE_SHUTDOWN} 关闭</li>
     * </ol>
     */
    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    private volatile int workerState;

    /**
     * 时间轮每次跳动（tick）的时间
     */
    private final long tickDuration;

    /**
     * 时间轮，内部的元素是双向链表
     */
    private final HashedWheelBucket[] wheel;

    /**
     * 为 wheel.length - 1，当做掩码使用
     */
    private final int mask;

    /**
     * 用于通知工作线程启动
     */
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);

    /**
     * 暂存任务，任务将会在下一个 tick 中被放置到正确的位置
     * @see Worker#transferTimeoutsToBuckets()
     */
    private final Queue<HashedWheelTimeout> timeouts = new LinkedBlockingQueue<>();

    /**
     * 用于存放被取消的任务，这些任务会在下个 tick 中被移除
     */
    private final Queue<HashedWheelTimeout> cancelledTimeouts = new LinkedBlockingQueue<>();
    private final AtomicLong pendingTimeouts = new AtomicLong(0);
    private final long maxPendingTimeouts;

    /**
     * 工作线程的启动时间
     */
    private volatile long startTime;

    /**
     * 使用默认线程工厂、默认刻度时长和默认每轮刻度数创建一个新的定时器。
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * 使用默认线程工厂和默认每轮刻度数创建一个新的定时器。
     *
     * @param tickDuration 刻度之间的时长
     * @param unit         {@code tickDuration} 的时间单位
     * @throws NullPointerException     如果 {@code unit} 为 {@code null}
     * @throws IllegalArgumentException 如果 {@code tickDuration} 小于等于 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * 使用默认线程工厂创建一个新的定时器。
     *
     * @param tickDuration  刻度之间的时长
     * @param unit          {@code tickDuration} 的时间单位
     * @param ticksPerWheel 时间轮的大小（每轮的刻度数）
     * @throws NullPointerException     如果 {@code unit} 为 {@code null}
     * @throws IllegalArgumentException 如果 {@code tickDuration} 或 {@code ticksPerWheel} 小于等于 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * 使用默认刻度时长和默认每轮刻度数创建一个新的定时器。
     *
     * @param threadFactory 用于创建后台执行 {@link TimerTask} 的 {@link ThreadFactory}
     * @throws NullPointerException 如果 {@code threadFactory} 为 {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * 使用默认每轮刻度数创建一个新的定时器。
     *
     * @param threadFactory 用于创建后台执行 {@link TimerTask} 的 {@link ThreadFactory}
     * @param tickDuration  刻度之间的时长
     * @param unit          {@code tickDuration} 的时间单位
     * @throws NullPointerException     如果 {@code threadFactory} 或 {@code unit} 为 {@code null}
     * @throws IllegalArgumentException 如果 {@code tickDuration} 小于等于 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * 创建一个新的定时器。
     *
     * @param threadFactory 用于创建后台执行 {@link TimerTask} 的 {@link ThreadFactory}
     * @param tickDuration  刻度之间的时长
     * @param unit          {@code tickDuration} 的时间单位
     * @param ticksPerWheel 时间轮的大小（每轮的刻度数）
     * @throws NullPointerException     如果 {@code threadFactory} 或 {@code unit} 为 {@code null}
     * @throws IllegalArgumentException 如果 {@code tickDuration} 或 {@code ticksPerWheel} 小于等于 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, -1);
    }

    /**
     * 创建一个新的定时器。
     *
     * @param threadFactory      用于创建后台执行 {@link TimerTask} 的 {@link ThreadFactory}
     * @param tickDuration       刻度之间的时长
     * @param unit               {@code tickDuration} 的时间单位
     * @param ticksPerWheel      时间轮的大小（每轮的刻度数）
     * @param maxPendingTimeouts 最大待处理超时任务数。超过此限制后调用 {@code newTimeout} 将抛出
     *                           {@link java.util.concurrent.RejectedExecutionException}。
     *                           如果此值为 0 或负数，则表示不限制最大待处理超时任务数。
     * @throws NullPointerException     如果 {@code threadFactory} 或 {@code unit} 为 {@code null}
     * @throws IllegalArgumentException 如果 {@code tickDuration} 或 {@code ticksPerWheel} 小于等于 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel,
            long maxPendingTimeouts) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }

        // 将 ticksPerWheel 归一化为 2 的幂并初始化时间轮
        wheel = createWheel(ticksPerWheel);
        mask = wheel.length - 1;

        // 将 tickDuration 转换为纳秒
        this.tickDuration = unit.toNanos(tickDuration);

        // 防止溢出
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                    "tickDuration: %d (expected: 0 < tickDuration in nanos < %d",
                    tickDuration, Long.MAX_VALUE / wheel.length));
        }
        workerThread = threadFactory.newThread(worker);

        this.maxPendingTimeouts = maxPendingTimeouts;

        if (INSTANCE_COUNTER.incrementAndGet() > INSTANCE_COUNT_LIMIT &&
                WARNED_TOO_MANY_INSTANCES.compareAndSet(false, true)) {
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // 该对象即将被垃圾回收，此时已无法执行正常的关闭流程。
            // 如果尚未关闭，则需要确保减少活跃实例计数。
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
            }
        }
    }

    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException(
                    "ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException(
                    "ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = ticksPerWheel - 1;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 1;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 2;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 4;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 8;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 16;
        return normalizedTicksPerWheel + 1;
    }

    /**
     * 显式启动后台线程。即使未调用此方法，后台线程也会在需要时自动启动。
     * Worker 启动与运行方法为 {@link Worker#run()}
     *
     * @throws IllegalStateException 如果此定时器已被 {@linkplain #stop() 停止}
     */
    public void start() {
        switch (WORKER_STATE_UPDATER.get(this)) {
            case WORKER_STATE_INIT:
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // 等待 worker 初始化 startTime
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // 忽略
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }

        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it be always 2.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                workerThread.interrupt();
                try {
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            INSTANCE_COUNTER.decrementAndGet();
        }
        return worker.unprocessedTimeouts();
    }

    @Override
    public boolean isStop() {
        return WORKER_STATE_SHUTDOWN == WORKER_STATE_UPDATER.get(this);
    }

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts ("
                    + pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
                    + "timeouts (" + maxPendingTimeouts + ")");
        }

        start();

        // 将任务放入阻塞队列，放入阻塞队列中的元素会在下一个 tick 中被放到时间轮的正确位置
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * 获取当前 Timer 的待处理任务数
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        String resourceType = ClassUtils.simpleClassName(HashedWheelTimer.class);
        logger.error("You are creating too many " + resourceType + " instances. " +
                resourceType + " is a shared resource that must be reused across the JVM," +
                "so that only a few instances are created.");
    }

    private final class Worker implements Runnable {

        /**
         * 用于存储未执行的 {@link Timeout}（在执行前被取消或在时间轮 STOP 时未执行）
         */
        private final Set<Timeout> unprocessedTimeouts = new HashSet<>();

        /**
         * 记录当前处于第一个 tick，可通过 tick & mask 来计算索引值
         */
        private long tick;

        @Override
        public void run() {
            // 初始化 startTime
            startTime = System.nanoTime();
            if (startTime == 0) {
                //【防错编程】，防止 System.nanoTime() 返回 0，Dubbo 将 0 作为工作线程未启动的标识
                startTime = 1;
            }

            // CountDownLatch 对象，给通知等待 Worker 启动的线程通知启动成功
            startTimeInitialized.countDown();

            do {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    int idx = (int) (tick & mask);
                    processCancelledTasks();
                    HashedWheelBucket bucket = wheel[idx];
                    transferTimeoutsToBuckets();
                    bucket.expireTimeouts(deadline);
                    tick++;
                }
            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // 运行结束前，将还未执行且未被取消的任务放入 unprocessedTimeouts 中
            for (HashedWheelBucket bucket : wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            for (; ; ) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            // 处理被取消的任务
            processCancelledTasks();
        }

        /**
         * 将 {@link HashedWheelTimer#timeouts} 中暂存的任务转移到正确的 Bucket 中
         */
        private void transferTimeoutsToBuckets() {
            // 每个刻度最多转移 100000 个超时任务，防止当线程在循环中不断添加新超时时导致工作线程停滞。
            for (int i = 0; i < 100000; i++) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    continue;
                }

                long calculated = timeout.deadline / tickDuration;
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                // calculated 可能小于 tick，这些任务也在当前 tick 中处理
                final long ticks = Math.max(calculated, tick);
                int stopIndex = (int) (ticks & mask);

                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }

        /**
         * 处理被取消的任务
         * 通过 {@link HashedWheelTimeout#cancel()} 取消任务
         */
        private void processCancelledTasks() {
            for (; ; ) {
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    break;
                }
                try {
                    timeout.remove();
                } catch (Throwable t) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * 根据 startTime 和当前 tick 数计算目标纳秒时间，
         * 然后等待直到达到该目标时间。
         *
         * @return 如果收到关闭请求则返回 Long.MIN_VALUE，
         * 否则返回当前时间（若原值为 Long.MIN_VALUE 则加 1 后返回）
         */
        private long waitForNextTick() {
            long deadline = tickDuration * (tick + 1);

            for (; ; ) {
                final long currentTime = System.nanoTime() - startTime;
                // 转化成 ms（向上取整）
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }
                if (isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        /**
         * 返回未执行的任务
         * @return unprocessedTimeouts
         */
        Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    private static final class HashedWheelTimeout implements Timeout {

        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");

        private final HashedWheelTimer timer;
        private final TimerTask task;
        private final long deadline;

        @SuppressWarnings({"unused", "FieldMayBeFinal", "RedundantFieldInitialization"})
        private volatile int state = ST_INIT;

        /**
         * RemainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
         * HashedWheelTimeout will be added to the correct HashedWheelBucket.
         */
        long remainingRounds;

        /**
         * This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
         * As only the workerThread will act on it there is no need for synchronization / volatile.
         */
        HashedWheelTimeout next;
        HashedWheelTimeout prev;

        /**
         * The bucket to which the timeout was added
         */
        HashedWheelBucket bucket;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean cancel() {
            // 只有 ST_INIT 状态的任务才能被取消，取消会在下个 tick 中被处理
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // 如果任务需要被取消，我们将其放入另一个队列，该队列将在每个 tick 中被处理。
            // 这意味着我们将拥有最多一个刻度时长的 GC 延迟。
            timer.cancelledTimeouts.add(this);
            return true;
        }

        void remove() {
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;
            String simpleClassName = ClassUtils.simpleClassName(this.getClass());

            StringBuilder buf = new StringBuilder(192)
                    .append(simpleClassName)
                    .append('(')
                    .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining)
                        .append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining)
                        .append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ")
                    .append(task())
                    .append(')')
                    .toString();
        }
    }

    /**
     * 用于存储 {@link HashedWheelTimeout} 的桶。这些超时任务以类似链表的数据结构存储，
     * 以便能够轻松地从中间移除 {@link HashedWheelTimeout}。
     * 此外，{@link HashedWheelTimeout} 本身充当节点，因此无需创建额外的对象。
     */
    private static final class HashedWheelBucket {

        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * 向当前 Bucket 添加 {@link HashedWheelTimeout}
         */
        void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * 处理当前 Bucket 中的任务
         * <ol>
         *     <li>若任务到期，执行并移除</li>
         *     <li>若任务被取消，直接移除</li>
         *     <li>非当前轮次的任务，round--</li>
         * </ol>
         */
        void expireTimeouts(long deadline) {
            HashedWheelTimeout timeout = head;

            while (timeout != null) {
                HashedWheelTimeout next = timeout.next;
                if (timeout.remainingRounds <= 0) {
                    next = remove(timeout);
                    if (timeout.deadline <= deadline) {
                        timeout.expire();
                    } else {
                        // 任务被防止到了错误的槽（应当永远不会发生）
                        throw new IllegalStateException(String.format(
                                "timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
                    }
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    timeout.remainingRounds--;
                }
                timeout = next;
            }
        }

        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * 从时间轮中抽取出未过期和未被取消的任务并返回
         */
        void clearTimeouts(Set<Timeout> set) {
            for (; ; ) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        /**
         * 获取并移除 Bucket 中第一个任务
         * @return head 任务
         */
        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // 防止 head 影响整个 Bucket 的 GC
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }

    private boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.US).contains("win");
    }
}
