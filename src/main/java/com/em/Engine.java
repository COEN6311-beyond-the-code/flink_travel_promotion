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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Engine {
    public static void main(String[] args) throws Exception {
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

    public static class MockEventSource extends org.apache.flink.streaming.api.functions.source.RichSourceFunction<Event> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // Define a list of predefined events
            List<Event> predefinedEvents = new ArrayList<>();
            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category B", 1002, 1L, 1));
            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category B", 1002, 1L, 2));
            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category B", 1002, 3L, 1));
//            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category A", 1001, 2L));
//            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category3", "3", "3"));
            Event event1 = predefinedEvents.get(0);
            Event event2 = predefinedEvents.get(1);
            Event event3 = predefinedEvents.get(2);

            Thread.sleep(2000);
            event1.setTimestamp(System.currentTimeMillis());
            ctx.collect(event1);

            Thread.sleep(2000);
            event1.setTimestamp(System.currentTimeMillis());
            ctx.collect(event1);

            Thread.sleep(2000);
            event1.setTimestamp(System.currentTimeMillis());
            ctx.collect(event1);

            Thread.sleep(10000);
            event1.setTimestamp(System.currentTimeMillis());
            ctx.collect(event1);

//            Thread.sleep(2000);
//            event1.setTimestamp(System.currentTimeMillis());
//            ctx.collect(event2);

            Thread.sleep(50000);

//
//            Thread.sleep(500);
//            event.setTimestamp(System.currentTimeMillis());
//            ctx.collect(event);
            // Send the predefined events
            // Optionally, you can keep sending the same events in a loop
//            while (running) {
//                for (Event event : predefinedEvents) {
//                    Thread.sleep(4000);
//                    event.setTimestamp(System.currentTimeMillis());
//                    ctx.collect(event);
//                    // Simulate the interval between events
//                }
//            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
