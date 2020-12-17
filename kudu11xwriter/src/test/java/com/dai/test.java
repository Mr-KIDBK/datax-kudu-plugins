package com.dai;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.q1.datax.plugin.writer.kudu11xwriter.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kudu.client.AsyncKuduClient.LOG;

/**
 * @author daizihao
 * @create 2020-08-28 11:03
 **/
public class test {


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        List<String> src = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20");


        int i = (src.size() - 1) / 8 + 1;
        int size = src.size() / i;
        List<List<String>> ls = new ArrayList<>(i);
        System.out.println("i: " + i + "  size : " + size);
        for (int j = 0; j < i - 1; j++) {
            ArrayList<String> destList = new ArrayList<>(src.subList(j * size, (j + 1) * size));
            ls.add(destList);
        }
        ArrayList<String> destList = new ArrayList<>(src.subList(size * (i - 1), src.size()));
//        System.arraycopy(src,  size*(i-1), destList, 0,src.size() % size +i size);
        ls.add(destList);

        ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(ls.size(),
                        ls.size(),
                        60L,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<Runnable>(),
                        new ThreadFactory() {
                            private final ThreadGroup group = System.getSecurityManager() == null ? Thread.currentThread().getThreadGroup() : System.getSecurityManager().getThreadGroup();
                            private final AtomicInteger threadNumber = new AtomicInteger(1);

                            @Override
                            public Thread newThread(Runnable r) {
                                Thread t = new Thread(group, r,
                                        "pool-kudu_rows_add-thread-" + threadNumber.getAndIncrement(),
                                        0);
                                if (t.isDaemon())
                                    t.setDaemon(false);
                                if (t.getPriority() != Thread.NORM_PRIORITY)
                                    t.setPriority(Thread.NORM_PRIORITY);
                                return t;
                            }
                        }, new ThreadPoolExecutor.AbortPolicy());
        for (int j = 0; j < 5; j++) {
//            CountDownLatch countDownLatch = new CountDownLatch(ls.size());
        for (List<String> l : ls) {
            System.out.println(l);
            Future<?> submit = threadPoolExecutor.submit(() -> {
                for (String s : l) {
                    System.out.println(s);
                }
//                countDownLatch.countDown();
                System.out.println(Thread.currentThread().getName());

            });


        }

//            countDownLatch.await();
            System.out.println("--------------------------------------------");
//            new Thread(()->{
//                for (String s : l) {
//                    System.out.println(s);
//                }
//            }).start();
        }
        threadPoolExecutor.shutdown();


    }
}
