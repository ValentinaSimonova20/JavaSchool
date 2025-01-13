package sbp.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import sbp.util.HashSum;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Консьюмер вычитывающий события с хешсуммами из топика "check-topic"
 * все вычитанные хешсуммы кладет во временное хранилище allHashSums
 */
public class ConsumerService {

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();// 1

    private final ConcurrentLinkedQueue<Long> allHashSums;

    private static final Logger logger = Logger.getLogger(ConsumerService.class.getName());

    public ConsumerService(ConcurrentLinkedQueue<Long> allHashSums) {
        this.allHashSums = allHashSums;
    }

    public void read(Properties properties) {
        int counter = 0;
        try(KafkaConsumer<String, HashSum> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(List.of(properties.getProperty("check-topic")));
            while (true) {
                ConsumerRecords<String, HashSum> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, HashSum> record: consumerRecords) {
                    logger.info("topic: " + record.topic());
                    logger.info("offset: " + record.offset());
                    logger.info("partition: " + record.partition());
                    logger.info("value: " + record.value());
                    logger.info("groupId: " + kafkaConsumer.groupMetadata().groupId());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    allHashSums.add(record.value().getHashSum());
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
