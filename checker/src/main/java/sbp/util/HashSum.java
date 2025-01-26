package sbp.util;

import java.sql.Timestamp;

public class HashSum {
    private Timestamp timestamp;
    private int hashSum;

    public HashSum() {
    }


    public HashSum(int hashSum, Timestamp timestamp) {
        this.hashSum = hashSum;
        this.timestamp = timestamp;
    }

    public int getHashSum() {
        return hashSum;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }
}
