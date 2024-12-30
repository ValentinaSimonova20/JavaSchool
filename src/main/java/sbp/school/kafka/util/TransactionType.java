package sbp.school.kafka.util;

public enum TransactionType {
    PRODUCTS(0), SERVICE(2), CLOTHES(3), TRANSPORT(4);

    TransactionType(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    private final int partitionNum;

    public int getPartitionNum() {
        return partitionNum;
    }
}
