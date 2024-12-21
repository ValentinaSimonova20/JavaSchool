package sbp.school.kafka.util;

import java.util.Date;

public class Transaction {
    private String type;
    private long sum;
    private String account;
    private Date date;

    public Transaction(String type, long sum, String account, Date date) {
        this.type = type;
        this.sum = sum;
        this.account = account;
        this.date = date;
    }

    public String getType() {
        return type;
    }

    public long getSum() {
        return sum;
    }

    public String getAccount() {
        return account;
    }

    public Date getDate() {
        return date;
    }
}
