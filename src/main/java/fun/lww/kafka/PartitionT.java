package fun.lww.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 设置自定义分区 需要在配置文件中添加这个分区类
 */
public class PartitionT implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //主题下几个分区
        int i = cluster.partitionCountForTopic(topic);
        if (i >= 2)
            return 1;
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
