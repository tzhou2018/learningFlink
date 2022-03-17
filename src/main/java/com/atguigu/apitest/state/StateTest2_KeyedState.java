package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/3/14 17:01
 **/
public class StateTest2_KeyedState {
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

        // 定义一个有状态的map操作，统计当前分区数据个数
        DataStream<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());
        resultStream.print();

        env.execute();
    }

    // 自定义实现RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> keyCountState;
        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        //    //        ValueState<Integer> valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));
        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));
//            super.open(parameters);

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其他状态api调用
            for (String str : myListState.get()) {
                System.out.println(str);
            }

            myListState.add("hello");
            myMapState.get("1");
            myMapState.put("2", 123.3);
            myReducingState.add(value);
//            Iterable<String> strings = myListState.get();

            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;


        }
    }
}
