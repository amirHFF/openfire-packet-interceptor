package io.projectZ.eventListener.kafka;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 8:33 PM
*/

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jivesoftware.util.NamedThreadFactory;

import java.util.concurrent.*;

public class ProducerQueueThreadPool {
    private final static Logger logger = LogManager.getLogger(ProducerQueueThreadPool.class);
    private static int corePoolSize = 2;
    private static int maxPoolSize = 4;
    private static int queueCapacity = 1000;


    public ProducerQueueThreadPool() {
    }

    public static ThreadPoolExecutor create(int corePoolSize , int maxPoolSize ,int queueCapacity) {


        BlockingQueue<Runnable> queue =
                new ArrayBlockingQueue<>(queueCapacity);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                queue,
                new NamedThreadFactory(
                        "message-event-worker",
                        Executors.defaultThreadFactory(),
                        false,
                        Thread.NORM_PRIORITY
                ),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
    public static ThreadPoolExecutor create() {


        BlockingQueue<Runnable> queue =
                new ArrayBlockingQueue<>(queueCapacity);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                queue,
                new NamedThreadFactory(
                        "message-event-worker",
                        Executors.defaultThreadFactory(),
                        false,
                        Thread.NORM_PRIORITY
                ),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    public static void shutdown(ThreadPoolExecutor executor) {

        logger.info("Shutting down MessageEventHandler...");

        executor.shutdown();

        try {

            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {

                logger.warn(
                        "MessageEventHandler did not terminate within 30 seconds. Forcing shutdown."
                );

                executor.shutdownNow();

                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.error("MessageEventHandler could not be terminated.");
                }
            }

        } catch (InterruptedException e) {

            logger.warn("Shutdown interrupted.", e);

            executor.shutdownNow();

            Thread.currentThread().interrupt();
        }

        logger.info("MessageEventHandler shutdown completed.");
    }
}

