package com.contactsunny.poc.KafkaConsumerWithKafkaModule;

import com.contactsunny.poc.kafkaModule.KafkaModule;
import com.contactsunny.poc.kafkaModule.exceptions.ValidationException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class App implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {

        /*
        Create properties with Kafka servers and group ID information
         */
        Properties properties = new Properties();
        properties.put("kafkaBootstrapServers", "localhost:9092");
        properties.put("groupId", "thetechcheck");
        properties.put("zookeeperHost", "localhost:2181");

        try {
            /*
            Craete a KafkaModule object, another object of a class which implements
            the KafkaConsumerImplementation interface, and then listen to the
            topic using the listenToTopic() method.
             */
            KafkaModule kafkaModule = new KafkaModule(properties);
            CustomKafkaConsumer customKafkaConsumer = new CustomKafkaConsumer();
            kafkaModule.listenToTopic("thetechcheck", customKafkaConsumer);
        } catch (ValidationException e) {
            e.printStackTrace();
        }

    }
}
