package sbp.consumer;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ThreadListener extends Thread{

    private Properties properties;
    private ConsumerService consumerService;

    public ThreadListener(Properties properties, ConcurrentLinkedQueue<Long> allHashSums) {
        this.properties = properties;
        this.consumerService = new ConsumerService(allHashSums);
    }

    private void listen() {
        consumerService.read(properties);
    }

    @Override
    public void run() {
        listen();
    }
}