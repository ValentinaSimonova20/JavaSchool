package sbp.util;

import java.sql.Timestamp;

public class Transaction {
    private Long id;
    private String transactionJson;
    private Timestamp creationDate;

    public Transaction(Long id, String transactionJson, Timestamp creationDate) {
        this.id = id;
        this.transactionJson = transactionJson;
        this.creationDate = creationDate;
    }

    public Long getId() {
        return id;
    }

    public String getTransactionJson() {
        return transactionJson;
    }

    public Timestamp getCreationDate() {
        return creationDate;
    }
}