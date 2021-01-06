import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        settings.put("auto.commit.interval.ms", 5000);
        settings.put("auto.offset.reset", "earliest");
        settings.put("key.deserializer", StringDeserializer.class.getName());
        settings.put("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, String>(settings);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for(ConsumerRecord<String, String> record: records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    System.out.println(this.id + ": " + data);

                }
            }
        } catch (WakeupException e) {

        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

    public static void main(String[] args) {

    }
}
