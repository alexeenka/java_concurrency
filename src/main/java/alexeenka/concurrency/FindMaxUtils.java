package alexeenka.concurrency;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("WeakerAccess")
public class FindMaxUtils {

    public static void main(String args[]) {
        int[] arr = RandomUtils.randomArr(100_000_000, 0, 300_000_000);

        {
            // warm up
            Arrays.stream(arr).max().getAsInt();
            Arrays.stream(arr).max().getAsInt();
        }

        {
            long startTime = System.currentTimeMillis();
            int max = findMaxStream(arr);

            System.out.println("Stream: Val: " + max + ". Time: " + (System.currentTimeMillis() - startTime));
        }
        {
            long startTime = System.currentTimeMillis();
            int max = findMaxParallelStream(arr);

            System.out.println("Parallel Stream: Val: " + max + ". Time: " + (System.currentTimeMillis() - startTime));
        }
        {
            long startTime = System.currentTimeMillis();
            int max = findMaxBarrier(arr);

            System.out.println("Barrier: Val: " + max + ". Time: " + (System.currentTimeMillis() - startTime));
        }
        {
            long startTime = System.currentTimeMillis();
            int max = findMaxWithLatch(arr);

            System.out.println("Latch: Val: " + max + ". Time: " + (System.currentTimeMillis() - startTime));
        }

    }

    public static int findMaxParallelStream(int[] arr) {
        return Arrays.stream(arr).parallel().max().getAsInt();
    }

    public static int findMaxStream(int[] arr) {
        return Arrays.stream(arr).max().getAsInt();
    }

    public static int findMaxWithLatch(int[] arr) {
        final int parallelism = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

        try {
            CountDownLatch latch = new CountDownLatch(parallelism);

            int partitionSize = arr.length / parallelism;
            final AtomicInteger globalMax = new AtomicInteger(arr[0]);

            for (int i = 0; i < parallelism; i++) {
                final int j = i;
                executorService.submit(() -> {
                    int max = findMaxBetween(arr, j * partitionSize, j * partitionSize + partitionSize);
                    //System.out.println(max);
                    try {
                        //System.out.println("#" + Thread.currentThread().getId() + ": wait " + max);
                        latch.countDown();
                        //System.out.println("#" + Thread.currentThread().getId() + ": after: " + max);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (globalMax.get() < max) {
                        //System.out.println("#" + Thread.currentThread().getId() + ": compare: " + max);
                        globalMax.set(max);
                    }
                });
            }

            // await current thread
            try {
                boolean result = latch.await(100, TimeUnit.SECONDS);
                if (!result) {
                    throw new RuntimeException("Can't find the max");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return globalMax.get();
        } finally {
            executorService.shutdown();
        }
    }

    public static int findMaxBarrier(int[] arr) {
        final int parallelism = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
        try {

            CyclicBarrier cyclicBarrier = new CyclicBarrier(parallelism + 1);

            int partitionSize = arr.length / parallelism;
            final AtomicInteger globalMax = new AtomicInteger(arr[0]);

            for (int i = 0; i < parallelism; i++) {
                final int j = i;
                executorService.submit(() -> {
                    int max = findMaxBetween(arr, j * partitionSize, j * partitionSize + partitionSize);
                    //System.out.println(max);
                    try {
                        //System.out.println("#" + Thread.currentThread().getId() + ": wait " + max);
                        cyclicBarrier.await();
                        //System.out.println("#" + Thread.currentThread().getId() + ": after: " + max);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (globalMax.get() < max) {
                        //System.out.println("#" + Thread.currentThread().getId() + ": compare: " + max);
                        globalMax.set(max);
                    }
                });
            }

            // await current thread
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            executorService.shutdown();

            return globalMax.get();

        } finally {
            executorService.shutdown();
        }
    }

    private static int findMaxBetween(int[] arr, int start, int stop) {
        int result = arr[start];

        if (stop > arr.length) {
            stop = arr.length;
        }

        for (int i = start + 1; i < stop; i++) {
            if (arr[i] > result) {
                result = arr[i];
            }
        }
        return result;
    }
}
