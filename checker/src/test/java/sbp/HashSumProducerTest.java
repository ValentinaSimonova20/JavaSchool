package sbp;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import sbp.producer.ProducerService;
import sbp.serializer.HashSumSerializer;
import sbp.util.HashSum;

import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HashSumProducerTest {

    MockProducer<String, HashSum> producer;

    ProducerService producerService;

    @BeforeAll
    void init() {
        this.producer = new MockProducer<>(
                true,
                new StringSerializer(),
                new HashSumSerializer()
        );
        this.producerService = new ProducerService(producer);
    }

    @Test
    void successTest() {
        HashSum sentHashSum = new HashSum(2, new Timestamp(123));
        producerService.send(sentHashSum);
        assertEquals(1, producer.history().size());
        HashSum actualHashSum = producer.history().get(0).value();
        assertEquals(sentHashSum.getHashSum(), actualHashSum.getHashSum());
        assertEquals(sentHashSum.getTimestamp(), actualHashSum.getTimestamp());
        producer.clear();
    }

    @Test
    void exceptionTest() {
        this.producer = new MockProducer<>(
                false,
                new StringSerializer(),
                new HashSumSerializer()
        );
        this.producerService = new ProducerService(producer);

        Future<RecordMetadata> metadata =
                producerService.sendHashSumAndReturnHashSum(new HashSum(3, new Timestamp(123)));

        producer.errorNext(new RuntimeException("error"));

        assertThrows(ExecutionException.class, metadata::get);
    }
}
