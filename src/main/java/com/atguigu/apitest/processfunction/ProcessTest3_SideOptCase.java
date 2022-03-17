package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/3/16 22:46
 **/
public class ProcessTest3_SideOptCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从socket文本流获取数据
        DataStream<String> inputStream = env.socketTextStream("36.134.134.186", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个OutputTag, 用来表示输出低温流

        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {

        };
        // 测试keyedProcessFunction 先分组然后自定义处理
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        // 判断温度，大于30度，高温输出到主流；低温输出的侧输出流
                        if (value.getTemperature() > 30) {
                            out.collect(value);
                        } else {
                            ctx.output(lowTempTag, value);
                        }
                    }
                });
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }
}
