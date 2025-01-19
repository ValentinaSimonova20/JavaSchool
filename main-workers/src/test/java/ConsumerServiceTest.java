import org.junit.jupiter.api.Test;
import sbp.school.kafka.consumer.ThreadListener;
import sbp.school.kafka.util.ConfigKafka;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerServiceTest {

    @Test
    void successTest() {

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        while (true) {
            List.of("test-group-1", "test-group-2")
                    .forEach(
                            groupId -> executorService.submit(new ThreadListener(ConfigKafka.getConsumerConfig(groupId)))
                    );
        }
    }
}
