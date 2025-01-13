package sbp.scheduler;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Шедулер, который периодично вычитывает из базы все транзакции за определенное время,
 * высчитывает их хэшсуммы и сравнивает с теми,
 * которые получили в топике обратного потока (они хранятся в поле allHashSums)
 */
public class RetryBrokenTransactionsScheduler {

    private ConcurrentLinkedQueue<Long> allHashSums;

    public RetryBrokenTransactionsScheduler(ConcurrentLinkedQueue<Long> allHashSums) {
        this.allHashSums = allHashSums;
    }

    public void schedule() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new SchedulerThread(allHashSums), 10, 10, TimeUnit.SECONDS);
    }
}
