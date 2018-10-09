package fun.lww.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

@Component
public class ProducerT {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void send(String msg) {
        System.out.println("生产者产出消息："+msg);
        kafkaTemplate.send("test-topic", "1----- hello, kafka "+msg+" "+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        kafkaTemplate.send("test-topic", "2----- hello, kafka "+msg+" "+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        kafkaTemplate.send("test", "测试数据");
    }

    public static void send2() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.93.220.67:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> prodocker = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            prodocker.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
        }
        prodocker.close();
    }

    public void send3(String msg) {
        KafkaTemplate.send("test-topic", msg, new Callback(){
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("发送正常");
                } else {
                    System.out.println("发送失败");
                    //发送失败 重试
                    
                }
            }
        });
    }
    
    public static void main(String[] args) {
        send2();
    }
}
