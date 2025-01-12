package sbp.scheduler;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetryBrokenTransactionsScheduler {

    private ConcurrentLinkedQueue<Long> allHashSums;

    public RetryBrokenTransactionsScheduler(ConcurrentLinkedQueue<Long> allHashSums) {
        this.allHashSums = allHashSums;
    }

    public void schedule() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.schedule(new SchedulerThread(allHashSums), 10, TimeUnit.SECONDS);
    }
}
