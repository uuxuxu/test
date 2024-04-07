package org.example.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Test02 {


    public void starter() throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 水印生成的间隔时间 5毫秒
        // 将间隔设置为0将禁用周期性水印发射
        env.getConfig().setAutoWatermarkInterval(5L);
        // 并行度
        env.setParallelism(5);
        // 2.数据来源
        DataStreamSource<String> datasource = env.fromElements(
                "1", "2", "3", "345345", "$5745457"
        );
        // 3.数据处理 - 时间策略指定
        SingleOutputStreamOperator<String> streamOperator = datasource.assignTimestampsAndWatermarks(
                // 设定事件时间戳无序的界限 这里是5毫秒
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(5))
                        .withTimestampAssigner(
                                // 为元素分配时间戳 从事件数据中抽取
                                new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String event, long recordTimestamp) {
                                        System.out.println("event = " + event);
                                        System.out.println("recordTimestamp = " + recordTimestamp);
                                        return recordTimestamp;
                                    }
                                })
        );
        // 4.sink输出
        streamOperator.print();
        // 5.任务执行
        env.execute();
    }


    public static void main(String[] args) throws Exception {
        Test02 test02 = new Test02();
        test02.starter();
    }

}
