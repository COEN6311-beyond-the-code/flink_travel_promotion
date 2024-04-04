package com.em.transform;

import com.em.pojo.Event;
import com.em.pojo.Rule;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class EventProcessingFunction extends KeyedProcessFunction<Tuple3<Long, Integer, String>, Event, Tuple2<Long, String>> {

    private List<Rule> rules;
    private transient MapState<Tuple3<Long, Integer, String>, Long> windowSizeState;
    private transient MapState<Tuple3<Long, Integer, String>, Long> windowCountState;

    public EventProcessingFunction(List<Rule> rules) {
        this.rules = rules;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 windowSizeState
        TupleTypeInfo<Tuple3<Long, Integer, String>> tupleTypeInfo = new TupleTypeInfo<>(
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        MapStateDescriptor<Tuple3<Long, Integer, String>, Long> windowSizeStateDescriptor = new MapStateDescriptor<>(
                "windowSizes",
                tupleTypeInfo,
                BasicTypeInfo.LONG_TYPE_INFO);
        windowSizeState = getRuntimeContext().getMapState(windowSizeStateDescriptor);

        // 初始化 windowCountState
        MapStateDescriptor<Tuple3<Long, Integer, String>, Long> windowCountStateDescriptor = new MapStateDescriptor<>(
                "windowCounts",
                tupleTypeInfo,
                BasicTypeInfo.LONG_TYPE_INFO);
        windowCountState = getRuntimeContext().getMapState(windowCountStateDescriptor);
    }

    @Override
    public void processElement(Event event, Context context, Collector<Tuple2<Long, String>> collector) throws Exception {
        // 获取(userId, itemId, category)对应的窗口大小
        Tuple3<Long, Integer, String> key = Tuple3.of(event.getUserId(), event.getItemId(),
                event.getCategory());
        Long windowSize = windowSizeState.get(key);

        // 如果窗口大小为空，则从规则中获取
        if (windowSize == null) {
            for (Rule rule : rules) {
                if (rule.getItemId().equals(event.getItemId()) && rule.getCategory().equals(event.getCategory())) {
                    windowSize = Long.valueOf(rule.getWindowsTime());
                    windowSizeState.put(key, windowSize);
                    break;
                }
            }
        }

        Long currentCount = windowCountState.get(key);
        if (currentCount == null) {
            currentCount = 0L;
        }
        currentCount += 1;
        windowCountState.put(key, currentCount);

        if (currentCount > 3) {
            collector.collect(Tuple2.of(key.f0, key.f2 + ": " + currentCount));
        }

        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + windowSize);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        for (Map.Entry<Tuple3<Long, Integer, String>, Long> entry : windowCountState.entries()) {
            windowCountState.remove(entry.getKey());
        }
    }
}
