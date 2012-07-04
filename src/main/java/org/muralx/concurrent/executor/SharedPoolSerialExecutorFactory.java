package org.muralx.concurrent.executor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link SerialExecutor} factory which creates Executors that share an
 * underlying Executor (or thread pool).
 * <p>
 * 
 * The only reason for the existence of this factory is to simplify the creation
 * of {@link SerialExecutor}s using the same default values and underlying
 * executor.
 * <p>
 * 
 * Tasks submitted on a {@link SerialExecutor} will be processed sequentially
 * and in the exact same order in which they were submitted, regardless of the
 * number of threads available in the underlying executor.
 * 
 * @author muralx (Diego Belfer)
 */
public class SharedPoolSerialExecutorFactory {
    public static final int MAX_EXECUTORS_FOR_ARRAY_QUEUE = (2 << 16);
    private final Executor underlyingExecutor;
    private final int expectedQueueSizePerSerialExecutor;
    private final int maxQueueSizePerSerialExecutor;

    /**
     * Creates a SharedPoolSerialExecutorFactory using the given arguments as
     * default values to instantiate all {@link SerialExecutor}s that will be
     * created by this factory.
     * <p>
     * Additional arguments will be used to create the underlying
     * {@link ThreadPoolExecutor} and the queue required by it.
     * <p>
     * 
     * @param expectedQueueSizePerSerialExecutor
     *            The expected size of the queue of each {@link SerialExecutor}
     *            created by this factory.
     * @param maxQueueSizePerSerialExecutor
     *            The max size of the queue of each {@link SerialExecutor}
     *            created by this factory.
     * @param maxSerialExecutorsCount
     *            The max number of executors this factory will create. 0
     *            implies unknown. This parameter will determine the kind of
     *            queue provided to the underlying ThreadPoolExecutor. If value
     *            == 0 or value >= {@code MAX_EXECUTORS_FOR_ARRAY_QUEUE} an
     *            unbounded queue will be used, Any other non negative value
     *            will force the use of a bounded queue.
     * @param corePoolSize
     *            the number of threads to keep in the pool, even if they are
     *            idle, unless {@code allowCoreThreadTimeOut} is set
     * @param maximumPoolSize
     *            the maximum number of threads to allow in the pool
     * @param keepAliveTime
     *            when the number of threads is greater than the core, this is
     *            the maximum time that excess idle threads will wait for new
     *            tasks before terminating.
     * @param unit
     *            the time unit for the {@code keepAliveTime} argument
     * 
     * @see ThreadPoolExecutor
     * @see SerialExecutor
     */
    public SharedPoolSerialExecutorFactory(int expectedQueueSizePerSerialExecutor, int maxQueueSizePerSerialExecutor,
            int maxSerialExecutorsCount, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {

        this(expectedQueueSizePerSerialExecutor, maxQueueSizePerSerialExecutor, new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize, keepAliveTime, unit, createUnderlyingExecutorQueue(maxSerialExecutorsCount)));
    }

    /**
     * Creates a SharedPoolSerialExecutorFactory using the given arguments as
     * default values to instantiate all {@link SerialExecutor}s created by this
     * factory.
     * <p>
     * 
     * @param expectedQueueSizePerSerialExecutor
     *            The expected size of the queue of each {@link SerialExecutor}
     *            created by this factory.
     * @param maxQueueSizePerSerialExecutor
     *            The max size of the queue of each {@link SerialExecutor}
     *            created by this factory.
     * @param underlyingExecutor
     *            The underlying executor to use for processing tasks submitted
     *            to any {@link SerialExecutor} created by this factory
     * 
     * @see SerialExecutor
     */
    protected SharedPoolSerialExecutorFactory(int expectedQueueSizePerSerialExecutor, int maxQueueSizePerSerialExecutor,
            Executor underlyingExecutor) {

        this.expectedQueueSizePerSerialExecutor = expectedQueueSizePerSerialExecutor;
        this.maxQueueSizePerSerialExecutor = maxQueueSizePerSerialExecutor;
        this.underlyingExecutor = underlyingExecutor;
    }

    public ExecutorService createSerialExecutor() {
        return new SerialExecutor(underlyingExecutor, expectedQueueSizePerSerialExecutor, maxQueueSizePerSerialExecutor);
    }

    public Executor getUnderlyingExecutor() {
        return underlyingExecutor;
    }

    public int getExpectedQueueSizePerSerialExecutor() {
        return expectedQueueSizePerSerialExecutor;
    }

    public int getMaxQueueSizePerSerialExecutor() {
        return maxQueueSizePerSerialExecutor;
    }

    private static BlockingQueue<Runnable> createUnderlyingExecutorQueue(int maxSerialExecutorsCount) {
        if (maxSerialExecutorsCount == 0 || maxSerialExecutorsCount > MAX_EXECUTORS_FOR_ARRAY_QUEUE) {
            /*
             * Undefined number of SerialExecutor or big number of them, we need
             * an unbounded queue
             */
            return new LinkedBlockingQueue<Runnable>();
        } else {
            /*
             * Use a fixed size queue
             */
            return new ArrayBlockingQueue<Runnable>(maxSerialExecutorsCount);
        }
    }
}
