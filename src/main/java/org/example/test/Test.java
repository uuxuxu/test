package org.example.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.function.DemoTransactionSource;

public class Test {


    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置运行模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 为流式作业启用检查点 以毫秒为单位 流式数据流的分布式状态将被定期快照
        env.enableCheckpointing(5000);

        // 2.设置自定义数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(new DemoTransactionSource(), "测试用的数据源");

        // 3.数据处理
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        // 4.数据输出
        stringSingleOutputStreamOperator.print();

        // 5.执行程序
        env.execute();

    }
}
