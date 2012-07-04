package org.muralx.concurrent.executor;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Simple test case for {@link SharedPoolSerialExecutorFactory}
 * 
 * @author muralx
 */
public class SharedPoolSerialExecutorFactoryTest {
    private static final int TASKS_COUNT = 1000;
    private static final int EXECUTORS_COUNT = 200;

    @Test
    public void testConcurrency() throws InterruptedException {
        SharedPoolSerialExecutorFactory factory = new SharedPoolSerialExecutorFactory(TASKS_COUNT, TASKS_COUNT,EXECUTORS_COUNT, 10, 20, 10, TimeUnit.SECONDS);
        ExecutorService[] executors = new ExecutorService[EXECUTORS_COUNT];
        
        final AtomicInteger count = new AtomicInteger(EXECUTORS_COUNT * TASKS_COUNT);
        final ConcurrentHashMap<Integer, Integer> totals = new ConcurrentHashMap<Integer, Integer>(EXECUTORS_COUNT * 2);
        final ConcurrentHashMap<Integer, AtomicBoolean> inUseFlag = new ConcurrentHashMap<Integer, AtomicBoolean>(EXECUTORS_COUNT * 2);
        
        for (int i = 0; i < EXECUTORS_COUNT; i++) {
            final Integer key = Integer.valueOf(i);
            totals.put(key, new Integer(0));
            inUseFlag.put(key, new AtomicBoolean());
            executors[i] = factory.createSerialExecutor();
            
            for (int j = 0; j < TASKS_COUNT; j++) {
                final Integer jValue = Integer.valueOf(j);
                executors[i].execute(new Runnable() {
                    public void run() {
                        if (!inUseFlag.get(key).compareAndSet(false, true)) {
                            throw new IllegalStateException("It was already set");
                        }
                        Thread.yield();
                        totals.put(key, totals.get(key).intValue() + jValue.intValue());
                        if (!inUseFlag.get(key).compareAndSet(true, false)) {
                            throw new IllegalStateException("It was already unset");
                        }
                        
                        count.decrementAndGet();
                    }
                });
            }
        }
        
        //Wait for completion
        long initialTime = System.currentTimeMillis();
        while (count.get() > 0 && System.currentTimeMillis() - initialTime < 1000) {
            Thread.sleep(1);
        }
        Assert.assertEquals(0, count.get());
        
        //Check results
        int expectedResult = (TASKS_COUNT * (TASKS_COUNT - 1)) / 2;
        Collection<Integer> values = totals.values();
        for (Integer v : values) {
            if (v.intValue() != expectedResult) {
                throw new IllegalStateException("SUM was : " + v + " expected: " + expectedResult);
            }
        }
        ((ExecutorService) factory.getUnderlyingExecutor()).shutdown();
    }
}
