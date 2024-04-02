package com.em;

import com.em.pojo.Event;
import com.em.pojo.Rule;
import com.em.source.MySQLSource;
import com.em.transform.CountCategoryItem;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        // 从 Mock 事件源生成模拟事件数据流
        DataStream<Event> eventDataStream = env.addSource(new MockEventSource());
        DataStream<Event> keyedEventDataStream = eventDataStream.keyBy(Event::getUserId);
        eventDataStream.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedEventDataStream
                .flatMap(new CountCategoryItem(rules)); // 传入rules

        // 打印结果
        resultStream.print();

        env.execute("CountCategoryItem Example");

        env.execute("Flink MySQL In-Memory Example");
    }

    public static class MockEventSource extends org.apache.flink.streaming.api.functions.source.RichSourceFunction<Event> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // Define a list of predefined events
            List<Event> predefinedEvents = new ArrayList<>();
            predefinedEvents.add(new Event( System.currentTimeMillis(), "Category B", "1002", "1"));
//            predefinedEvents.add(new Event(System.currentTimeMillis(), "Category2", "2", "2"));
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
