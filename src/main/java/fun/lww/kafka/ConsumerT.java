package fun.lww.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
public class ConsumerT {

    @KafkaListener(topics = {"test", "test-topic"})
    public void consumer(String msg) {
        System.out.println("消费者消费消息："+msg);
    }

    public static void consumer2() {
        //手动提交offset
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.93.220.67:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));
        final int minBatchSize = 20;
        List<ConsumerRecord<String, String>> list = new ArrayList<ConsumerRecord<String, String>>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                list.add(record);
                System.out.printf("offset= %d key= %s value= %s", record.offset(), record.key(), record.value());
                System.out.println();
            }
            if (list.size() > minBatchSize) {
                //提交offset
                consumer.commitSync();
                System.out.println(list.size());
                list.clear();
                System.out.println(list.size());
            }
        }

        //自动提交offset
        /*Properties props = new Properties();
        props.put("bootstrap.servers", "47.93.220.67:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }*/
    }

    public static void main(String[] args) {
        consumer2();
    }
}
