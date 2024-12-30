package sbp.school.kafka.consumer;
import java.util.Properties;

public class ThreadListener extends Thread{

    private Properties properties;
    private ConsumerService consumerService;

    public ThreadListener(Properties properties) {
        this.properties = properties;
        this.consumerService = new ConsumerService();
    }

    private void listen() {
        consumerService.read(properties);
    }

    @Override
    public void run() {
        listen();
    }
}