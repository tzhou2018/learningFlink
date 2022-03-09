package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/2/24 15:24
 **/
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        // 从文件读取
        DataStream<String> inputStream = env.readTextFile("D:\\Users\\zhoutong55\\IdeaProjects\\LearnFlink\\src\\main\\resources\\sensor.txt");
        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        // 使用java8特性 lambda 表达式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));

        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(data -> data.getId());

        // 滚动聚合，获取当前最大温度值
        SingleOutputStreamOperator<SensorReading> resultStream = keyStream.maxBy("temperature");
        resultStream.print();


        env.execute();
    }
}
