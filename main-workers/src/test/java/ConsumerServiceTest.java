import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.ConsumerService;
import sbp.school.kafka.consumer.ThreadListener;
import sbp.school.kafka.util.ConfigKafka;
import sbp.school.kafka.util.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerServiceTest {

    private static final String TOPIC = "topic";

    private MockConsumer<String, Transaction> consumer;
    private List<Transaction> updates;
    private ConsumerService consumerService;
    private static final int PARTITION = 0;

    private Throwable pollException;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        updates = new ArrayList<>();
        consumerService = new ConsumerService(consumer, updates::add, ex-> this.pollException = ex);
    }

    @Test
    @Disabled
    void successTest() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        while (true) {
            List.of("test-group-1", "test-group-2")
                    .forEach(
                            groupId -> executorService.submit(
                                    new ThreadListener(
                                            ConfigKafka.getConsumerConfig(groupId),
                                            new KafkaConsumer<>(ConfigKafka.getConsumerConfig(groupId)),
                                            updates::add
                                    )
                            )
                    );
        }
    }

    @Test
    void readSuccessTest() {
        consumer.schedulePollTask(
                () -> {
                    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
                    consumer.addRecord(
                            new ConsumerRecord<>(TOPIC, PARTITION, 0L, "bank", new Transaction())
                    );
                }

        );

        consumer.schedulePollTask(() -> consumerService.stop());

        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        startingOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startingOffsets);

        consumerService.read(TOPIC);
        assertEquals(1, updates.size());
        assertTrue(consumer.closed());
    }

    @Test
    void readExceptionTest() {
        consumer.schedulePollTask(
                () -> {
                    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
                    consumer.setPollException(new KafkaException("poll exception"));
                }

        );

        consumer.schedulePollTask(() -> consumerService.stop());

        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        startingOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startingOffsets);
        consumerService.read(TOPIC);
        assertTrue(pollException instanceof KafkaException);
        assertEquals("poll exception", pollException.getMessage());
        assertTrue(consumer.closed());
    }
}
