package com.dll.flink01;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class flink01 {
    public static void main(String[] args) throws Exception {
        //获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置flink相关配置
        //1.设置平行度
        env.setParallelism(1);
        //2.设置checkpoint 间隔及方式
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //3.设置状态后端方式
        env.setStateBackend(new HashMapStateBackend());
        //4.设置checkpoint地址
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Flink/src/snapshot");

        //配置数据源
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\Flink\\src\\main\\resources\\data.txt");
        stringDataStreamSource.print();

        env.execute("flink01");


    }
}
