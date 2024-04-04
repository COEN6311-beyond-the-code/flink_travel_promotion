package com.em;

import com.em.pojo.Event;
import com.em.pojo.Result;
import com.em.pojo.Rule;
import com.em.sink.RabbitMQSink;
import com.em.source.MySQLSource;
import com.em.source.RabbitMQSource;
import com.em.transform.BroadcastStateDescriptors;
import com.em.transform.MyKeyedBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Engine {
    public static void main(String[] args) throws Exception {
        //promotion system
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> eventDataStream = env
//                .addSource(new MockEventSource())
                .addSource(new RabbitMQSource()).setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        // Key by userId, itemId, and category
        KeyedStream<Event, Tuple3<Long, Integer, String>> keyedStream = eventDataStream
                .keyBy(new KeySelector<Event, Tuple3<Long, Integer, String>>() {
                    @Override
                    public Tuple3<Long, Integer, String> getKey(Event event) throws Exception {
                        return new Tuple3<>(event.userId, event.itemId, event.category);
                    }
                });

        DataStream<Rule> ruleDataStream = env.addSource(new MySQLSource());
        BroadcastStream<Rule> ruleBroadcastStream = ruleDataStream.broadcast(BroadcastStateDescriptors.RULES_BROADCAST_STATE_DESCRIPTOR);
        SingleOutputStreamOperator<Result> process = keyedStream
                .connect(ruleBroadcastStream)
                .process(new MyKeyedBroadcastProcessFunction());
        eventDataStream.print();
        process.addSink(new RabbitMQSink<>());
        env.execute("Event Processing Job");

    }
}
