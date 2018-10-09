package fun.lww.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Random;

@SpringBootApplication
@EnableScheduling
public class KafkaApplication {

    @Autowired
    private ProducerT producerT;

    @Scheduled(fixedRate = 1000*5)
    public void test() {
        System.out.println();
        producerT.send(""+new Random().nextDouble());
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }


}
