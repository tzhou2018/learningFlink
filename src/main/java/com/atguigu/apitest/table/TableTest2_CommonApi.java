package com.atguigu.apitest.table;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/3/24 17:26
 **/
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

//        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

//        // 1.2 基于老版本planner的批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);
//
//        // 1.3 基于Blink的流处理
//        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
//
//        // 1.4 基于blink的批处理
//        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);
        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String inputPath = "D:\\Users\\zhoutong55\\IdeaProjects\\LearnFlink\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(inputPath))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");
        Table inputTable = tableEnv.from("inputTable");
//        tableEnv.toAppendStream(inputTable, Row.class).print();
//        inputTable.printSchema();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id,temp")
                .filter("id = 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");
        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");
        env.execute();
    }
}
