package sbp.school.kafka.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class TransactionPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int size = partitionInfoList.size();
        // вычитываем партицию из типа транзакции
        int partitionNum = ((Transaction) value).getType().getPartitionNum();
        // если партиция превышает реальное количество партиций то записываем в последнюю
        if(partitionNum > size) {
            return size - 1;
        }
        return partitionNum;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
