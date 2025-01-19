package sbp.checkhash;

import sbp.producer.ProducerService;
import sbp.util.HashSum;
import sbp.util.TransactionDao;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

/**
 * Высчитывает хэшсуммы транзакций за определенный промежуток времени
 * и отправляет событие с информацией об этой хэшсумме
 */
public class CalculateHashSumRunnableTask implements Runnable{

    private final Properties properties;
    private final ProducerService producerService;

    public CalculateHashSumRunnableTask(Properties properties) {
        this.properties = properties;
        this.producerService = new ProducerService();

    }

    @Override
    public void run() {
        Timestamp now = Timestamp.from(Instant.now());
        producerService.send(
                new HashSum(
                        TransactionDao.getTransactionsHashSumInPeriod(
                                now, Integer.parseInt(properties.getProperty("time-stamp-hashsum-check-minutes"))
                        ),
                        now
                )
        );
    }
}
