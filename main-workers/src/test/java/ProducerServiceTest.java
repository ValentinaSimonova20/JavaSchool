import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import sbp.school.kafka.ProducerService;
import sbp.school.kafka.serializer.JsonTransactionSerializer;
import sbp.school.kafka.util.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProducerServiceTest {

    MockProducer<String, Transaction> producer;

    ProducerService producerService;

    @BeforeAll
    void init() {
        this.producer = new MockProducer<>(
                getMockedCluster(),
                true,
                new TransactionPartitioner(),
                new StringSerializer(),
                new JsonTransactionSerializer()
        );
        this.producerService = new ProducerService(producer);
    }

    @ParameterizedTest
    @MethodSource("testSource")
    @DisplayName("Тест добавления записи в определенную партицию в зависимости от типа транзакции")
    void success(Transaction transaction) {
        assertEquals(transaction.getType().getPartitionNum(), producerService.send(transaction).partition());
        assertEquals(1, producer.history().size());
        producer.clear();
    }

    @Test
    void sendMessage() {
        int sizeBefore = TransactionDao.getAllTransactions().size();
        new ProducerService().send(
                new Transaction(TransactionType.PRODUCTS, 1234, "счет1", LocalDateTime.now())
        );
        assertEquals(sizeBefore + 1, TransactionDao.getAllTransactions().size());
    }

    private List<Transaction> testSource() {
        return List.of(
                new Transaction(TransactionType.SERVICE, 1234, "счет1", LocalDateTime.now()),
                new Transaction(TransactionType.CLOTHES, 4321, "счет2", LocalDateTime.now()),
                new Transaction(TransactionType.PRODUCTS, 5555, "счет4", LocalDateTime.now()),
                new Transaction(TransactionType.TRANSPORT, 454545, "счет6", LocalDateTime.now())
        );
    }

    private Cluster getMockedCluster() {
        String topicName = ConfigKafka.getKafkaProperties().getProperty("topic");
        return new Cluster(
                "kafkab",
                new ArrayList<>(),
                List.of(
                        new PartitionInfo(topicName, 0, null, null, null),
                        new PartitionInfo(topicName, 1, null, null, null),
                        new PartitionInfo(topicName, 2, null, null, null),
                        new PartitionInfo(topicName, 3, null, null, null),
                        new PartitionInfo(topicName, 4, null, null, null)
                ),
                emptySet(),
                emptySet()
        );
    }
}
