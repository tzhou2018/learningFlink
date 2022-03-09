package com.atguigu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Description TODO
 * 浮窗显示查找快捷键 ctrl+F12
 * Author zhoutong
 * Date 2022/2/24 11:18
 **/
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件读取
        DataStream<String> inputStream = env.readTextFile("D:\\Users\\zhoutong55\\IdeaProjects\\LearnFlink\\src\\main\\resources\\sensor.txt");
        // 1. map, 把String转换成长度输出
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });


        // 2. flatmap, 按逗号切分字段
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        // 3. filter, 筛选sensor1_1 开头对应的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                return value.startsWith("sensor_1");
            }
        });


        // 打印输出
        mapStream.print("map");
        flatMapStream.print("map");
        filterStream.print("filter");


        env.execute();
    }
}
