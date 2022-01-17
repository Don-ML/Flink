package com.dll.flink02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class flinkkafka {
    public static void main(String[] args) throws Exception {
        //1.创建Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2。设置并行度
        env.setParallelism(1);
        //3。设置checkpoint¸
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //4.设置Kafka
        String topic="test";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id","flink02");

        //4.增加Kafkasource
        DataStreamSource<String> kafkasource = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties));
        kafkasource.print();
        env.execute("flinkkafka");
    }
}