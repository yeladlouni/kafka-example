import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class BasicConsumer implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public BasicConsumer(int id, String groupId, List<String> topics) {
        this.id = id;
        this.topics = topics;

        Properties settings = new Properties();
        settings.put("bootstrap.servers", "localhost:9092");
        settings.put("group.id", groupId);
        settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.consumer = new KafkaConsumer<String, String>(settings);
    }

    @Override
    public void run() {

    }

    public void shutdown() {
        consumer.wakeup();
    }
}
