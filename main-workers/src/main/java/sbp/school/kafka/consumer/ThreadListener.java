package sbp.school.kafka.consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.util.Transaction;

import java.util.Properties;
import java.util.logging.Logger;

public class ThreadListener extends Thread{

    private final Properties properties;
    private final ConsumerService consumerService;

    private static final Logger logger = Logger.getLogger(ThreadListener.class.getName());


    public ThreadListener(
            Properties properties,
            KafkaConsumer<String, Transaction> consumer,
            java.util.function.Consumer<Transaction> resultAccumulatorFunction
    ) {
        this.properties = properties;
        this.consumerService = new ConsumerService(
                consumer, resultAccumulatorFunction, ex -> logger.severe(ex.getMessage())
        );
    }

    private void listen() {
        consumerService.read(properties.getProperty("topic"));
    }

    @Override
    public void run() {
        listen();
    }
}