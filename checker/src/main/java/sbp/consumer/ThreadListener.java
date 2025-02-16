package sbp.consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.checkhash.CheckHashSumService;
import sbp.util.HashSum;

import java.util.Properties;
import java.util.logging.Logger;

public class ThreadListener extends Thread{

    private final Properties properties;
    private final ConsumerService consumerService;

    private static final Logger logger = Logger.getLogger(ThreadListener.class.getName());

    public ThreadListener(
            Properties properties,
            KafkaConsumer<String, HashSum> consumer,
            java.util.function.Consumer<HashSum> resultAccumulatorFunction
    ) {
        this.properties = properties;
        this.consumerService = new ConsumerService(
                new CheckHashSumService(), consumer, resultAccumulatorFunction, ex -> logger.severe(ex.getMessage())
        );
    }

    private void listen() {
        consumerService.read(properties);
    }

    @Override
    public void run() {
        listen();
    }
}