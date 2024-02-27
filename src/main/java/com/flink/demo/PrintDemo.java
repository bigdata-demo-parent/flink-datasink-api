package com.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink Sink Api Demo: print
 *
 * @author tang.xuandong
 * @version 1.0.0
 * @date 2024/2/27 10:53
 */
public class PrintDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<String, Integer>("flink", 1));
        list.add(new Tuple2<String, Integer>("sink", 1));
        list.add(new Tuple2<String, Integer>("artemisestia", 1));
        list.add(new Tuple2<String, Integer>("com", 1));
        list.add(new Tuple2<String, Integer>("xiaoxiaomo", 1));
        list.add(new Tuple2<String, Integer>("com", 1));

        env.fromCollection(list)
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute("Flink Sink Api Demo: print");

    }


}
