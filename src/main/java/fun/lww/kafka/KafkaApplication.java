package fun.lww.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaApplication {

    @Autowired
    private ProducerT producerT;

//    @Scheduled(fixedRate = 10000)
    public void test() {
        String msg = "test time: " + System.currentTimeMillis();
        System.out.println(msg);
        producerT.send(msg);
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }


}
