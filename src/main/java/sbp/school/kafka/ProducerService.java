package sbp.school.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.util.ConfigKafka;
import sbp.school.kafka.util.Transaction;

public class ProducerService {
    private final KafkaProducer<String, Transaction> kafkaProducer;

    public ProducerService() {
        this.kafkaProducer = new KafkaProducer<>(ConfigKafka.getKafkaProperties());
    }

    public void send() {

    }
}
