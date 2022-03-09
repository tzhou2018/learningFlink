package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * Descripion TODO
 * Author zhoutong
 * Date 2022/2/23 10:33
 **/
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度


        // 从 集合中读取数据
        List<SensorReading> dataStream = Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        );
        DataStream<SensorReading> dataStreamSource = env.fromCollection(dataStream);

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3, 67, 189);

        // 打印输出
        dataStreamSource.print("data");
        integerDataStream.print("int").setParallelism(1);

        // 执行任务
        env.execute("flink");
    }
}
