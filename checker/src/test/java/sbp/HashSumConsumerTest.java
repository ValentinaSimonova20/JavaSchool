package sbp;


import org.junit.jupiter.api.Test;
import sbp.consumer.ThreadListener;
import sbp.util.ConfigKafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HashSumConsumerTest {

    @Test
    void success() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        // слушаем топик обратного потока
        while (true) {
            List.of("test-group-1", "test-group-2")
                    .forEach(
                            groupId -> executorService.submit(
                                    new ThreadListener(ConfigKafka.getConsumerConfig(groupId))
                            )
                    );
        }
    }
}
