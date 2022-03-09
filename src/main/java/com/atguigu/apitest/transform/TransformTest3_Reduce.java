package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.IOException;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/2/24 16:55
 **/
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取
        DataStream<String> inputStream = env.readTextFile("./src/main/resources/sensor.txt");
        env.setParallelism(1);
        // 转换成SensorReading类型

        // 使用java8特性 lambda 表达式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));

        });

        // 分组
//        KeyedStream<SensorReading, Tuple> keyStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(data -> data.getId());
        KeyedStream<SensorReading, String> keyStream = dataStream.keyBy(SensorReading::getId);

        // reduce聚合
        // 滚动聚合，获取当前最大温度值
        SingleOutputStreamOperator<SensorReading> resultStream = keyStream.reduce(
                (curSensor, newSensor) -> new SensorReading(curSensor.getId(), newSensor.getTimestamp(), Math.max(curSensor.getTemperature(), newSensor.getTemperature()))
        );
        resultStream.print("result");
        env.execute();
    }
}
