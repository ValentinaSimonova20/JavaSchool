package sbp.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import sbp.checkhash.CheckHashSumService;
import sbp.util.HashSum;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Консьюмер вычитывающий события с хешсуммами из топика "check-topic"
 * все вычитанные хешсуммы кладет во временное хранилище allHashSums
 */
public class ConsumerService {

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();// 1

    private static final Logger logger = Logger.getLogger(ConsumerService.class.getName());

    private final CheckHashSumService checkHashSumService;

    private final Consumer<String, HashSum> consumer;

    private final java.util.function.Consumer<HashSum> resultAccumulatorFunction;

    private final java.util.function.Consumer<Throwable> exceptionConsumer;

    public ConsumerService(
            CheckHashSumService checkHashSumService,
            Consumer<String, HashSum> consumer,
            java.util.function.Consumer<HashSum> resultAccumulatorFunction,
            java.util.function.Consumer<Throwable> exceptionConsumer
    ) {
        this.checkHashSumService = checkHashSumService;
        this.consumer = consumer;
        this.resultAccumulatorFunction = resultAccumulatorFunction;
        this.exceptionConsumer = exceptionConsumer;
    }

    public void read(Properties properties) {
        int counter = 0;
        try(consumer) {
            consumer.subscribe(List.of(properties.getProperty("check-topic")));
            while (true) {
                ConsumerRecords<String, HashSum> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, HashSum> record: consumerRecords) {
                    logger.info("topic: " + record.topic());
                    logger.info("offset: " + record.offset());
                    logger.info("partition: " + record.partition());
                    logger.info("value: " + record.value());
                    logger.info("groupId: " + consumer.groupMetadata().groupId());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    checkHashSumService.checkHashSum(
                            record.value().getHashSum(),
                            record.value().getTimestamp(),
                            Integer.parseInt(properties.getProperty("time-stamp-hashsum-check-minutes"))
                    );
                    resultAccumulatorFunction.accept(record.value());
                    if (counter % 1000 == 0) {
                        logger.info("records commited");
                        consumer.commitSync(currentOffsets, null);
                    }
                    counter++;
                }
            }
        }catch (WakeupException e) {
            System.out.println("Shutting down...");
        }catch (RuntimeException ex) {
            exceptionConsumer.accept(ex);
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        consumer.wakeup();
    }
}
