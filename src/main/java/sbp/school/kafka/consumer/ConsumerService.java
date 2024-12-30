package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.util.Transaction;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class ConsumerService {

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();// 1

    private static final Logger logger = Logger.getLogger(ConsumerService.class.getName());

    public ConsumerService() {
    }

    public void read(Properties properties) {
        int counter = 0;
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
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if (counter % 1000 == 0) {
                        logger.info("records commited");
                        kafkaConsumer.commitSync(currentOffsets, null);
                        counter++;
                    }
                }
            }

        }
    }


}
