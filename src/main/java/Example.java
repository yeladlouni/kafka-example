import org.apache.kafka.common.errors.TimeoutException;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Example {
    public static void main(String[] args) throws InterruptedException {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        CountDownLatch latch = new CountDownLatch(2);
        BasicProducer producerThread = new BasicProducer(KafkaProperties.TOPIC, isAsync, null, false, 10000, -1, latch);
        producerThread.start();

        BasicConsumer consumerThread = new BasicConsumer(KafkaProperties.TOPIC, "DemoConsumer", Optional.empty(), false, 10000, latch);
        consumerThread.start();

        if (!latch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }

        consumerThread.shutdown();
        System.out.println("All finished!");
    }
}
