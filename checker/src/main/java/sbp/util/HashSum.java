package sbp.util;

import java.sql.Timestamp;

public class HashSum {
    private Timestamp timestamp;
    private int hashSum;

    public HashSum() {
    }


    public HashSum(int hashSum) {
        this.hashSum = hashSum;
    }

    public int getHashSum() {
        return hashSum;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }
}
