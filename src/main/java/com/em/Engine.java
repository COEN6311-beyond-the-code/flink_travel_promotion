package com.em;

import com.em.pojo.Event;
import com.em.pojo.Rule;
import com.em.source.MySQLSource;
import com.em.transform.BroadcastStateDescriptors;
import com.em.transform.MyKeyedBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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

        // Read data from MySQL into memory
        MySQLSource mySQLSource = new MySQLSource();
        mySQLSource.open(new Configuration()); //
        List<Rule> rules = mySQLSource.executeAndCollect();
        env.fromCollection(rules).map(new RichMapFunction<Rule, String>() {
            @Override
            public String map(Rule rule) throws Exception {
                return rule.toString();
            }
        }).print();
        DataStream<Event> eventDataStream = env
                .addSource(new MockEventSource())
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



        // 将ruleDataStream转换为广播流
        BroadcastStream<Rule> ruleBroadcastStream = env.fromCollection(rules).broadcast(BroadcastStateDescriptors.RULES_BROADCAST_STATE_DESCRIPTOR);

        // 将eventDataStream与广播流连接
        SingleOutputStreamOperator<String> process = keyedStream
                .connect(ruleBroadcastStream)
                .process(new MyKeyedBroadcastProcessFunction());
        eventDataStream.print();
        process.print();
        env.execute("Event Processing Job");

    }

    public static class MockEventSource extends org.apache.flink.streaming.api.functions.source.RichSourceFunction<Event> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // Define a list of predefined events
            List<Event> predefinedEvents = new ArrayList<>();
            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category B", 1002, 1L));
            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category B", 1002, 1L));
            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category A", 1001, 2L));
//            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category3", "3", "3"));

            // Send the predefined events
            for (Event event : predefinedEvents) {
                ctx.collect(event);
                // Simulate the interval between events
                Thread.sleep(1000);
            }

            // Optionally, you can keep sending the same events in a loop
            while (running) {
                for (Event event : predefinedEvents) {
                    event.setTimestamp(System.currentTimeMillis());
                    ctx.collect(event);
                    // Simulate the interval between events
                    Thread.sleep(1000);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
