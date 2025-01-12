package sbp.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import sbp.school.kafka.ProducerService;
import sbp.util.Transaction;
import sbp.util.TransactionDao;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SchedulerThread extends Thread{

    private ConcurrentLinkedQueue<Long> allHashSums;

    private ProducerService producerService;

    public SchedulerThread(ConcurrentLinkedQueue<Long> allHashSums) {
        this.allHashSums = allHashSums;
        this.producerService = new ProducerService();
    }

    @Override
    public void run() {
        List<Transaction> transactions = TransactionDao.getTransactionsInPeriod();

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
        objectMapper.registerModule(new JavaTimeModule());
        if(
                allHashSums
                        .poll()
                        .intValue() != Long.valueOf(transactions.stream().mapToLong(transaction -> transaction.getId()).sum()).hashCode()) {
            transactions
                    .stream()
                    .map(
                            transaction -> {
                                try {
                                    return objectMapper.readValue(
                                            transaction.getTransactionJson(),
                                            sbp.school.kafka.util.Transaction.class
                                    );
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    ).forEach(transaction -> producerService.send(transaction));
        }
    }


}
