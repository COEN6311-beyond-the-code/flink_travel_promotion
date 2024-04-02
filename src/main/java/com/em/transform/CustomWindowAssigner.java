package com.em.transform;

import com.em.pojo.Event;
import com.em.pojo.Rule;
import org.apache.flink.api.common.ExecutionConfig;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CustomWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    private final List<Rule> rules;

    public CustomWindowAssigner(List<Rule> rules) {
        this.rules = rules;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        // 获取元素的键值
        Event event = (Event) element;
        Tuple3<Long, Integer, String> key = Tuple3.of(event.getUserId(), event.getItemId(), event.getCategory());

        // 在规则集中查找对应的规则
        Long windowSize = findWindowSize(key.f1, key.f2);

        if (windowSize == null) {
            return Collections.emptyList();
        }
        // 返回一个包含指定时间戳的时间窗口的集合
        return Collections.singletonList(new TimeWindow(timestamp, timestamp + windowSize * 100));
    }

    private Long findWindowSize(Integer itemId, String category) {
        // 根据 itemId 和 category 在规则集中查找对应的窗口大小
        for (Rule rule : rules) {
            if (rule.getItemId().equals(itemId) && rule.getCategory().equals(category)) {
                return Long.valueOf(rule.getWindowsTime());
            }
        }
        return null;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        // 返回默认触发器，这里使用 ProcessingTime 触发器
        return ProcessingTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        // 返回窗口序列化器，这里使用默认序列化器
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

    @Override
    public String toString() {
        return "CustomWindowAssigner()";
    }
}
