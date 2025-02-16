package sbp;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import sbp.checkhash.CheckHashSumService;
import sbp.consumer.ConsumerService;
import sbp.consumer.ThreadListener;
import sbp.util.ConfigKafka;
import sbp.util.HashSum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;

public class HashSumConsumerTest {

    private List<HashSum> updates;

    private static final String TOPIC = "check-topic";

    private static final String TEST_CONSUMER_GROUP = "test";

    private MockConsumer<String, HashSum> consumer;
    private ConsumerService consumerService;
    private static final int PARTITION = 0;

    private Throwable pollException;

    @BeforeEach
    void setUp() {
        CheckHashSumService checkHashSumService = Mockito.mock(CheckHashSumService.class);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        updates = new ArrayList<>();
        consumerService = new ConsumerService(checkHashSumService, consumer, updates::add, ex-> this.pollException = ex);
        doNothing().when(checkHashSumService).checkHashSum(eq(0), any(), eq(10));
    }

    @Test
    @Disabled
    void success() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        // слушаем топик обратного потока
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
                            new ConsumerRecord<>(TOPIC, PARTITION, 0L, "default", new HashSum())
                    );
                }

        );

        consumer.schedulePollTask(() -> consumerService.stop());

        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        startingOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startingOffsets);
        consumerService.read(ConfigKafka.getConsumerConfig(TEST_CONSUMER_GROUP));
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
        consumerService.read(ConfigKafka.getConsumerConfig(TEST_CONSUMER_GROUP));
        assertTrue(pollException instanceof KafkaException);
        assertEquals("poll exception", pollException.getMessage());
        assertTrue(consumer.closed());
    }
}
