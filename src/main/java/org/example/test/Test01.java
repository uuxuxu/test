package org.example.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.function.DemoPartitioner;
import org.example.function.DemoTransactionSource;

import javax.annotation.PostConstruct;
import java.util.List;

public class Test01 {

    @PostConstruct
    public void starter() throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置运行模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2.设置自定义数据源
        DataStreamSource<String> stringDataStreamSource = env.addSource(new DemoTransactionSource(), "测试用的数据源");
        // 3.数据处理
        DataStream<String> dataStream = stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).partitionCustom(new DemoPartitioner(), new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });
        // 4.数据输出
        dataStream.print();
        // 5.执行程序
        env.execute();
    }


    public void starterBro() throws Exception {
        // 准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 被广播的数据
        DataSource<String> dataSource = env.fromElements("5", "6", "7", "8");

        // 常规数据
        DataSet<String> dataSet = env.fromElements("哈哈哈哈1", "哈哈哈哈2", "哈哈哈哈3", "哈哈哈哈4");

        // 数据处理
        // 使用 RichMapFunction, 在open() 方法中拿到广播变量
        // 由于我是在单个节点上去拿变量的 所以其实放在map方法里面也可以 但是分布式环境下还是得从open方法里获取比较好吧
        dataSet.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                List<String> broadcastVariable = getRuntimeContext().getBroadcastVariable("被广播的共享变量名");
                System.out.println("print=" + broadcastVariable);
            }
        }).withBroadcastSet(dataSource, "被广播的共享变量名").print();
    }


    /**
     * 累加器
     *
     * @throws Exception
     */
//    public void starterInt() throws Exception {
//        // 准备环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 设置并行度
//        env.setParallelism(1);
//        // 常规数据
//        DataStream<String> dataSet = env.fromElements("哈哈哈哈1", "哈哈哈哈2", "哈哈哈哈3", "哈哈哈哈4");
//        // 新建累加器
//        IntCounter counter = new IntCounter();
//        // 数据处理
//        dataSet.map(new RichMapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                counter.add(1);
//                return value;
//            }
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                // 累加器添加进运行上下文
//                getRuntimeContext().addAccumulator("counter", counter);
//            }
//        }).print();
//        // 执行作业的结果
//        JobExecutionResult jobExecutionResult = env.execute();
//        // 获取累加器
//        Integer result = jobExecutionResult.getAccumulatorResult("counter");
//        System.out.println("累加器之和是=" + result);
//
//    }


    public static void main(String[] args) throws Exception {
        Test01 test01 = new Test01();
        test01.starterBro();
    }

}
