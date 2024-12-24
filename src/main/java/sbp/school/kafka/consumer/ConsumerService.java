package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.util.Transaction;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerService {

    private static final Logger logger = Logger.getLogger(ConsumerService.class.getName());

    public ConsumerService() {
    }

    public void read(Properties properties) {
        try(KafkaConsumer<String, Transaction> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(List.of(properties.getProperty("topic")));
            while (true) {
                ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, Transaction> record: consumerRecords) {
                    logger.info("topic: " + record.topic());
                    logger.info("offset: " + record.offset());
                    logger.info("partition: " + record.partition());
                    logger.info("value: " + record.value());
                    logger.info("groupId: " + kafkaConsumer.groupMetadata().groupId());
                }
                kafkaConsumer.commitSync();
            }

        }
    }


}
