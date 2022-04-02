package com.atguigu.apitest.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Description TODO
 * Author zhoutong
 * Date 2022/3/29 18:28
 **/
public class TableTest3_FileOut {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // 4.输出到文件
        // 连接外部文件，注册输出表
        String outPath = "D:\\Users\\zhoutong55\\IdeaProjects\\LearnFlink\\src\\main\\resources\\Out.txt";
        tableEnv.connect(new FileSystem().path(outPath))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");
        resultTable.insertInto("outputTable");
        tableEnv.execute("");

    }
}
