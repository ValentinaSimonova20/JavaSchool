package sbp.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.util.ConfigKafka;
import sbp.util.HashSum;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class ProducerService {

    private final Producer<String, HashSum> kafkaProducer;
    private final Properties properties;

    private static final Logger logger = Logger.getLogger(ProducerService.class.getName());

    public ProducerService() {
        this.properties = ConfigKafka.getProducerConfig();
        this.kafkaProducer = new KafkaProducer<>(properties);

    }

    public void send(HashSum hashSum) {
        Future<RecordMetadata> result =
                kafkaProducer
                        .send(new ProducerRecord<>(properties.getProperty("check-topic"), hashSum), ((metadata, exception) -> {
                            if(exception != null) {
                                // В случае сбоя продюсер должен фиксировать в логе ошибку,
                                // смещение и партицию битого сообщения
                                logger.severe("Произошла ошибка: " + exception.getMessage());
                            } else {
                                logger.info("Успешная отправка сообщения");
                            }
                            logger.info("topic: " + metadata.topic());
                            logger.info("offset: " + metadata.offset());
                            logger.info("partition: " + metadata.partition());
                        }));
        try {
            // дожидаемся ответа от брокера чтобы не потерять сообщение и залогировать ошибку
            result.get();
        } catch (InterruptedException e) {
            logger.severe("InterruptedException " + e.getMessage());
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.severe("ExecutionException {}" + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
