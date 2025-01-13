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

    private final ConcurrentLinkedQueue<Long> allHashSums;

    private final ProducerService producerService;

    private final ObjectMapper objectMapper;

    public SchedulerThread(ConcurrentLinkedQueue<Long> allHashSums) {
        this.allHashSums = allHashSums;
        this.producerService = new ProducerService();
        this.objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void run() {
        List<Transaction> transactions = TransactionDao.getTransactionsInPeriod();

        // пытаемся получить хэшсумму, которая была добавлену в эту коллекцию из топика
        Long hashSum = allHashSums.poll();
        if(hashSum != null) {
            // если хешсуммы не совпали, то переотправляем все транзакции которые выяитали из базы
            if(hashSum.intValue() != Long.valueOf(transactions.stream().mapToLong(Transaction::getId).sum()).hashCode()) {
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
                        ).forEach(producerService::send);
            }
        }
    }


}
