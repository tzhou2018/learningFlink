package com.atguigu.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Parameter;

/**
 * Descripion TODO
 * Author zhoutong
 * Date 2022/1/29 9:30
 **/
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 从文件中读取数据
//        String inputPath = "D:\\Users\\zhoutong55\\IdeaProjects\\LearnFlink\\src\\main\\resources\\hello";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用 parameter tool 工具获取参数
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");

        // 从socket 文本流中读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream("36.134.134.186", 7777);
//        DataStreamSource<String> inputDataStream = env.socketTextStream("192.168.31.41", 7777);

        // 结构浮窗快捷键 Ctrl + F12
        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();

        // 执行任务
        env.execute();
    }
}
