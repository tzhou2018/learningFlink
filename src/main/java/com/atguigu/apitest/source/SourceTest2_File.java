package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Descripion TODO
 * Author zhoutong
 * Date 2022/2/23 14:05
 **/
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 从文件读取
        DataStream<String> dataStreamSource = env.readTextFile("D:\\Users\\zhoutong55\\IdeaProjects\\LearnFlink\\src\\main\\resources\\sensor.txt");

        // 打印输出
        dataStreamSource.print();

        env.execute();
    }
}
