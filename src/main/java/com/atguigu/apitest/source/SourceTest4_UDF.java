package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * Descripion TODO
 * Author zhoutong
 * Date 2022/2/23 17:33
 **/
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 从文件读取
        DataStream<SensorReading> dataStreamSource = env.addSource(new MySensorSource());

        // 打印输出
        dataStreamSource.print();

        env.execute();
    }

    // 实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {
        // 定义一个标志位，用来控制数据的产生
        private boolean running = true;


        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置十个传感器的初始温度值
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上做随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));

                }
                Thread.sleep(1000l);

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
