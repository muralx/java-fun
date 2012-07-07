package org.muralx.concurrent.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class SerialExecutorSample {
    /**
     * Number of tasks per serial executor
     */
    private static final int TASKS_COUNT = 1000;
    /**
     * Number of serial executors
     */
    private static final int EXECUTORS_COUNT = 200;
    /**
     * Keeps sum total for each serial executor
     */
    private static int[] totals = new int[EXECUTORS_COUNT];
    /**
     * 
     */
    private static ExecutorService[] executors = new ExecutorService[EXECUTORS_COUNT];

    public static void main(String[] args) throws InterruptedException {
        SharedPoolSerialExecutorFactory factory = new SharedPoolSerialExecutorFactory(TASKS_COUNT, TASKS_COUNT, EXECUTORS_COUNT,
                10, 20, 10, TimeUnit.SECONDS);
        
        //Create the executors
        for (int i = 0; i < EXECUTORS_COUNT; i++) {
            executors[i] = factory.createSerialExecutor();
        }
        
        //Submit the tasks to the proper executor
        for (int i = 0; i < EXECUTORS_COUNT; i++) {
            final int index = i;
            for (int j = 0; j < TASKS_COUNT; j++) {
                final int valueAdd = j;
                executors[i].execute(new Runnable() {
                    public void run() {
                        totals[index] += valueAdd;
                    }
                });
            }
        }

        //Wait for termination of all executors and print results
        for (int i = 0; i < EXECUTORS_COUNT; i++) {
            executors[i].shutdown();
            executors[i].awaitTermination(10000, TimeUnit.SECONDS);
            System.out.println(totals[i]);
        }
        
        //Shutdown underlying executor 
        ((ExecutorService) factory.getUnderlyingExecutor()).shutdown();
    }
}
