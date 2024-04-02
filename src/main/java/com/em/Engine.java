package com.em;

import com.em.pojo.Event;
import com.em.pojo.Rule;
import com.em.source.MySQLSource;
import com.em.transform.CountAggregateFunction;
import com.em.transform.CustomWindowAssigner;
import com.em.transform.RuleBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.InputStream;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

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
        DataStream<Event> eventDataStream = env.addSource(new MockEventSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 将规则转换为广播流
        SingleOutputStreamOperator<Rule> ruleBroadcastStream = env
                .fromCollection(rules);
        // 将事件流与广播流连接
        DataStream<Tuple2<Event, Rule>> connectedStream = eventDataStream.keyBy(
                        new KeySelector<Event, Object>() {
                            public Tuple3<Long, Integer, String> getKey(Event event) {
                                return Tuple3.of(event.getUserId(), event.getItemId(), event.getCategory());
                            }
                        }).connect(ruleBroadcastStream)
                .process(new RuleBroadcastProcessFunction());
        ruleBroadcastStream.print();
        eventDataStream.print();
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
