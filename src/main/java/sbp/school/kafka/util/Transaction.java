package sbp.school.kafka.util;

import java.time.LocalDateTime;

public class Transaction {
    private TransactionType type;
    private long sum;
    private String account;
    private LocalDateTime date;

    public Transaction(TransactionType type, long sum, String account, LocalDateTime date) {
        this.type = type;
        this.sum = sum;
        this.account = account;
        this.date = date;
    }

    public TransactionType getType() {
        return type;
    }

    public long getSum() {
        return sum;
    }

    public String getAccount() {
        return account;
    }

    public LocalDateTime getDate() {
        return date;
    }
}
