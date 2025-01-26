package sbp.checkhash;

import sbp.util.ConfigKafka;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Запускает таску по расчету хэшсуммы
 */
public class CalculateHashSumScheduler {

    public static void schedule() {
        Properties properties = ConfigKafka.getProducerConfig();
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(
                new CalculateHashSumRunnableTask(properties),
                0,
                Integer.parseInt(properties.getProperty("time-stamp-hashsum-check-minutes")),
                TimeUnit.MINUTES
        );
    }
}
