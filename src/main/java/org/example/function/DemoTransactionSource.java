package org.example.function;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义数据源  通过实现SourceFunction
 */
public class DemoTransactionSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (true) {
            // 发射元素
            ctx.collect(String.valueOf(new Random().nextInt(50)
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
    }
}

