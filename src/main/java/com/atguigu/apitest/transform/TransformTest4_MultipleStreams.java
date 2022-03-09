package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/2/24 20:32
 **/
public class TransformTest4_MultipleStreams {
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
        // 1. 分流， 按照温度值30度为界分为两条流
        // 1.12.1 版本丢弃了split方法

        DataStream<Object> process = dataStream.process(new ProcessFunction<SensorReading, Object>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<Object> out) throws Exception {

            }
        });

        env.execute();

    }
}
