package com.em.transform;

import com.em.pojo.Event;
import com.em.pojo.Rule;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Tuple3<Long, Integer, String>, Event, Rule, String> {

    // 使用ValueState来记录当前的计数
    private transient ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {
        // 初始化计数状态
        ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>(
                "countState", // 状态名称
                Types.INT); // 状态类型
        countState = getRuntimeContext().getState(countStateDescriptor);
    }

    @Override
    public void processElement(Event value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // 从广播状态中获取匹配的规则
        ReadOnlyBroadcastState<Void, Rule> broadcastState = ctx.getBroadcastState(BroadcastStateDescriptors.RULES_BROADCAST_STATE_DESCRIPTOR);
        Rule rule = broadcastState.get(null); // 获取规则，假设规则是全局唯一的

        // 根据规则立即更新计数
        Integer currentCount = countState.value();
        if (currentCount == null) {
            currentCount = 0;
        }
        currentCount += 1; // 更新计数
        countState.update(currentCount);

        // 根据规则进行判断，例如计数达到规则中的阈值
        if (rule != null && currentCount >= rule.getBrowseTimes()) {
            // 满足条件，执行相应操作
            out.collect("满足条件：对于键 " + ctx.getCurrentKey() + "，计数达到 " + currentCount);
            // 根据需要重置计数
            countState.clear();
        }

        // 可以根据需要设置定时器，例如清理状态
    }

    @Override
    public void processBroadcastElement(Rule rule, KeyedBroadcastProcessFunction<Tuple3<Long, Integer, String>, Event, Rule, String>.Context context, Collector<String> collector) throws Exception {

    }

}
