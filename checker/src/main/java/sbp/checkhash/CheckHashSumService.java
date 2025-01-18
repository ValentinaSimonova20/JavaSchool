package sbp.checkhash;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import sbp.school.kafka.ProducerService;
import sbp.util.Transaction;
import sbp.util.TransactionDao;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.logging.Logger;

public class CheckHashSumService{


    private static final Logger logger = Logger.getLogger(CheckHashSumService.class.getName());

    private final ProducerService producerService;

    private final ObjectMapper objectMapper;

    public CheckHashSumService() {
        this.producerService = new ProducerService();
        this.objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
        objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Сравнение переданной хэшсуммы с той, которая реально содержится в базе
     * если хешсуммы не совпали, то переотправляем все транзакции которые вычитали из базы
     * @param hashSum контрольная сумма по идентификаторам сообщений
     * @param timestamp время когда пришла контрольная сумма
     * @param timestampForCheckMinutes за какой интервал нужно проверять записи
     */
    public void checkHashSum(int hashSum, Timestamp timestamp, int timestampForCheckMinutes) {
        List<Transaction> transactions = TransactionDao.getTransactionsInPeriod(timestamp, timestampForCheckMinutes);
        if(hashSum != Long.valueOf(transactions.stream().mapToLong(Transaction::getId).sum()).hashCode()) {
            transactions
                    .stream()
                    .map(this::convertTransaction)
                    .forEach(producerService::send);
        }
    }

    private sbp.school.kafka.util.Transaction convertTransaction(Transaction transaction) {
        try {
            return objectMapper.readValue(
                    transaction.getTransactionJson(),
                    sbp.school.kafka.util.Transaction.class
            );
        } catch (JsonProcessingException e) {
            logger.severe("Произошла ошибка при конвертации транзакции:" + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
