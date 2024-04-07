package org.example.function;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区
 * @author 李家民
 */
public class DemoPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        System.out.println("目前分区总数=" + numPartitions + "  当前值=" + key + "  通过最左边的值看分区号");

        if (new Integer(key) > 20) {
            return 1;
        } else {
            return 2;
        }
    }
}

