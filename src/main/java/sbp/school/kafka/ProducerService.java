package sbp.school.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.util.ConfigKafka;
import sbp.school.kafka.util.Transaction;
import sbp.school.kafka.util.TransactionType;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerService {
    private final KafkaProducer<String, Transaction> kafkaProducer;

    public ProducerService() {
        this.kafkaProducer = new KafkaProducer<>(ConfigKafka.getKafkaProperties());
    }

    public void send() {
        send(TransactionType.SERVICE);
        send(TransactionType.PRODUCTS);
        send(TransactionType.TRANSPORT);
        send(TransactionType.CLOTHES);
    }

    private void send(TransactionType transactionType) {
        Future<RecordMetadata> result =
                kafkaProducer
                        .send(new ProducerRecord<>(
                                "quickstart-events-partitions",
                                new Transaction(transactionType, 100L, "счет", LocalDateTime.now())
                        ), ((metadata, exception) -> {
                            if(exception != null) {
                                // В случае сбоя продюсер должен фиксировать в логе ошибку,
                                // смещение и партицию битого сообщения
                                System.out.println("ExecutionException " + exception.getMessage());

                                System.out.println("offset: " + metadata.offset());
                                System.out.println("partition: "+ metadata.partition());
                            } else {
                                System.out.println("Успешная отправка сообщения");
                                System.out.println("offset: " + metadata.offset());
                                System.out.println("topic: " + metadata.topic());
                                System.out.println("partition: "+ metadata.partition());
                            }
                        }));
        try {
            // дожидаемся ответа от брокера чтобы не потерять сообщение и залогировать ошибку
            result.get();
        } catch (InterruptedException e) {
            System.out.println("InterruptedException " + e.getMessage());
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            System.out.println("ExecutionException " + e.getMessage());
            throw new RuntimeException(e);
        }

    }
}
