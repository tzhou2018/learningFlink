package com.atguigu.apitest.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/3/29 20:04
 **/
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 连接Kafka,读取数据
        tableEnv.connect(new Kafka()
                .version("0.12")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");
        // 3. 简单转换
        Table sensorTable = tableEnv.from("inputTable");
        // 简单转换
        Table resultTable = sensorTable.select("id,temp")
                .filter("id = 'sensor_6'");

        // 聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");
        // 4. 建立Kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("0.12")
                .topic("sinktest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");
        resultTable.insertInto("outputTable");
        env.execute();
    }
}
