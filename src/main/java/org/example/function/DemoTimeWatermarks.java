package org.example.function;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class DemoTimeWatermarks implements WatermarkGenerator {
    /**
     * 为每个事件调用，允许水印生成器检查并记住事件时间戳，或根据事件本身发出水印
     * @param event          接收的事件数据
     * @param eventTimestamp 事件时间戳
     * @param output         可用output.emitWatermark方法生成一个Watermark
     */
    @Override
    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {
        System.out.println(
                "event=" + event +
                        "   eventTimestamp=" + eventTimestamp +
                        "   WatermarkOutput=" + output
        );
    }



    /**
     * 周期性触发，可能会发出新的水印，也可能不会
     * 调用此方法和生成水印的时间间隔取决于ExecutionConfig.getAutoWatermarkInterval()
     * @param output 可用output.emitWatermark方法生成一个Watermark
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        System.out.println("被定期执行的方法onPeriodicEmit");
    }
}

