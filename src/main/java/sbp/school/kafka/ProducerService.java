package sbp.school.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sbp.school.kafka.util.ConfigKafka;
import sbp.school.kafka.util.Transaction;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerService {

    private static final Logger logger = LogManager.getLogger(ProducerService.class);

    private final Producer<String, Transaction> kafkaProducer;
    private final Properties properties;

    public ProducerService() {
        this(new KafkaProducer<>(ConfigKafka.getKafkaProperties()));
    }

    public ProducerService(Producer<String, Transaction> kafkaProducer) {
        this.properties = ConfigKafka.getKafkaProperties();
        this.kafkaProducer = kafkaProducer;
    }

    public RecordMetadata send(Transaction transaction) {

        Future<RecordMetadata> result =
                kafkaProducer
                        .send(new ProducerRecord<>(properties.getProperty("topic"), transaction), ((metadata, exception) -> {
                            if(exception != null) {
                                // В случае сбоя продюсер должен фиксировать в логе ошибку,
                                // смещение и партицию битого сообщения
                                logger.error("Произошла ошибка: {}", exception.getMessage());
                            } else {
                                logger.info("Успешная отправка сообщения");
                            }
                            logger.info("topic: {}", metadata.topic());
                            logger.info("offset: {}", metadata.offset());
                            logger.info("partition: {}", metadata.partition());
                        }));
        try {
            // дожидаемся ответа от брокера чтобы не потерять сообщение и залогировать ошибку
            return result.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException {}", e.getMessage());
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
