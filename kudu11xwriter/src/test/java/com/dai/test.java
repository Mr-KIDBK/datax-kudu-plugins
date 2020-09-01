package com.dai;

import com.q1.datax.plugin.writer.kudu11xwriter.Kudu11xHelper;
import org.junit.Test;
import com.q1.datax.plugin.writer.kudu11xwriter.ColumnType;
import com.q1.datax.plugin.writer.kudu11xwriter.InsertModeType;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kudu.client.AsyncKuduClient.LOG;

/**
 * @author daizihao
 * @create 2020-08-28 11:03
 **/
public class test {
    @Test
    public void kuduTypeTest() {
        AtomicInteger counter = new AtomicInteger(0);
        while (true) {

            if (counter.incrementAndGet() > 100) {
                System.out.println(counter);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                counter.set(0);
            }
        }
    }
}
