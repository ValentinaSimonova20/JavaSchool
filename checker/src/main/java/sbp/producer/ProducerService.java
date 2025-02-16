package sbp.producer;

import org.apache.kafka.clients.producer.*;
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

    public ProducerService(Producer<String, HashSum> kafkaProducer) {
        this.properties = ConfigKafka.getProducerConfig();
        this.kafkaProducer = kafkaProducer;

    }

    /**
     * Отправка сообщения в топик с информацией о хешсумме
     * В случае сбоя продюсер должен фиксировать в логе ошибку, смещение и партицию битого сообщения.
     * Дожидаемся ответа от брокера чтобы не потерять сообщение и залогировать ошибку
     * @param hashSum значение которое нужно отправить
     */
    public void send(HashSum hashSum) {
        Future<RecordMetadata> result = sendHashSumAndReturnHashSum(hashSum);
        try {
            result.get();
        } catch (InterruptedException e) {
            logger.severe("InterruptedException " + e.getMessage());
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.severe("ExecutionException {}" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Future<RecordMetadata> sendHashSumAndReturnHashSum(HashSum hashSum) {
        return kafkaProducer
                .send(new ProducerRecord<>(properties.getProperty("check-topic"), hashSum), (this::callback));
    }

    private void callback(RecordMetadata metadata, Exception exception) {
        if(exception != null) {
            logger.severe("Произошла ошибка: " + exception.getMessage());
        } else {
            logger.info("Успешная отправка сообщения");
        }
        logger.info("topic: " + metadata.topic());
        logger.info("offset: " + metadata.offset());
        logger.info("partition: " + metadata.partition());
    }
}
