package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ThreadedConsumerDemo {

    private static Logger logger = LoggerFactory.getLogger(ThreadedConsumerDemo.class);

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating a consumer thread");
        //set consumer properties
        Runnable runnable = new ConsumerThread(countDownLatch);

        //starting consumer thread
        Thread thread = new Thread(runnable);
        thread.start();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) runnable).shutdown();
        }));

        try {
            countDownLatch.await();
        } catch (
                InterruptedException e) {
            logger.error("Thread interrupted ", e);
        } finally {
            logger.info("Closing application");
        }
    }
}
