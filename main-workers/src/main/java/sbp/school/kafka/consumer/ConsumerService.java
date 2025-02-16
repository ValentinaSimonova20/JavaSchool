package sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import sbp.school.kafka.util.Transaction;
import sbp.school.kafka.util.TransactionDao;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ConsumerService {

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();// 1

    private static final Logger logger = Logger.getLogger(ConsumerService.class.getName());

    private final Consumer<String, Transaction> consumer;

    private final java.util.function.Consumer<Transaction> resultAccumulatorFunction;

    private java.util.function.Consumer<Throwable> exceptionConsumer;

    public ConsumerService(
            Consumer<String, Transaction> consumer,
            java.util.function.Consumer<Transaction> resultAccumulatorFunction,
            java.util.function.Consumer<Throwable> exceptionConsumer
    ) {
        TransactionDao.createTable("transactionsConsumer");
        this.consumer = consumer;
        this.resultAccumulatorFunction = resultAccumulatorFunction;
        this.exceptionConsumer = exceptionConsumer;
    }

    public void read(String topic) {
        int counter = 0;
        try(consumer) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, Transaction> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, Transaction> record: consumerRecords) {
                    logger.info("topic: " + record.topic());
                    logger.info("offset: " + record.offset());
                    logger.info("partition: " + record.partition());
                    logger.info("value: " + record.value());
                    logger.info("groupId: " + consumer.groupMetadata().groupId());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    TransactionDao.saveTransaction(record.value(), "transactionsConsumer");
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
