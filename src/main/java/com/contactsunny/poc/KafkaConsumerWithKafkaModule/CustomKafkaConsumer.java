package com.contactsunny.poc.KafkaConsumerWithKafkaModule;

import com.contactsunny.poc.kafkaModule.interfaces.KafkaConsumerImplementation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class CustomKafkaConsumer implements KafkaConsumerImplementation {

    @Override
    public void handleMessage(ConsumerRecord<String, String> consumerRecord, KafkaConsumer<String, String> kafkaConsumer) {

        String message = consumerRecord.value();

        System.out.println("Received message: " + message);

        Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

        commitMessage.put(new TopicPartition(consumerRecord.topic(),consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1));

        kafkaConsumer.commitSync(commitMessage);
    }
}
