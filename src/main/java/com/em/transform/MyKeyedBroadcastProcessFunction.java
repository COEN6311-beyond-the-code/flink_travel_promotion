package com.em.transform;

import com.alibaba.fastjson.JSON;
import com.em.pojo.Event;
import com.em.pojo.EventTypeEnum;
import com.em.pojo.Result;
import com.em.pojo.Rule;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Tuple3<Long, Integer, String>, Event, Rule, Result> {
    //save browse event time
    private transient ListState<Long> windowCountState;
    //milliSecond
    private transient ValueState<Integer> windowsTime;
    private transient ValueState<Integer> ruleId;
    //milliSecond

    private transient ValueState<Integer> waitingTime;
    //save latest browse timer trigger time

    private transient ValueState<Long> timerEventTime;
    //save browseAchievement trigger time
    private transient ListState<Long> browseAchievement;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<Long> windowCountStateDescriptor = new ListStateDescriptor<>(
                "windowCountState",
                Types.LONG);
        windowCountState = getRuntimeContext().getListState(windowCountStateDescriptor);
        browseAchievement = getRuntimeContext().getListState(new ListStateDescriptor<>(
                "browseAchievement",
                Types.LONG));
        windowsTime = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "windowsTime",
                Types.INT));
        waitingTime = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "waitingTime",
                Types.INT));
        timerEventTime = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "timerEventTime",
                Types.LONG));
        ruleId = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "ruleId",
                Types.INT));
    }

    @Override
    public void processElement(Event value, ReadOnlyContext ctx, Collector<Result> out) throws Exception {
        ReadOnlyBroadcastState<String, Rule> broadcastState = ctx.getBroadcastState(BroadcastStateDescriptors.RULES_BROADCAST_STATE_DESCRIPTOR);
        Rule rule = null;
        for (Map.Entry<String, Rule> entry : broadcastState.immutableEntries()) {
            Rule currentRule = entry.getValue();
            if (currentRule.category.equals(value.getCategory())
                    && currentRule.itemId.equals(value.getItemId())) {
                rule = currentRule;
                windowsTime.update(rule.getWindowsTime() * 1000);
                waitingTime.update(rule.getWaitTime() * 1000);
                ruleId.update(rule.getId());
            }
        }
        if (rule != null) {
            long eventTime = value.getTimestamp();
            Integer eventType = value.getType();
            if (eventType.equals(EventTypeEnum.VIEW.getType())) {
                //update browse Timer
                Long currentTimerTimestamp = timerEventTime.value();
                if (currentTimerTimestamp != null) {
                    ctx.timerService().deleteProcessingTimeTimer(currentTimerTimestamp + windowsTime.value());
                }
                ctx.timerService().registerProcessingTimeTimer(eventTime + windowsTime.value());
                timerEventTime.update(eventTime);

                windowCountState.add(eventTime);
                Integer validCount = clearOldEventAndRCalculateValidCount(eventTime);
                if (validCount >= rule.browseTimes) {
                    System.out.println("meet browse requirement");
                    ctx.timerService().registerProcessingTimeTimer(eventTime + waitingTime.value());
                    browseAchievement.addAll(Collections.singletonList(eventTime));
                }
            } else if (eventType.equals(EventTypeEnum.PURCHASE.getType())) {
                //clear all browseAchievement Timer
                clearNotifyTimer(value, ctx);
                windowCountState.clear();

            }

        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Result> out) throws Exception {
        if (timerEventTime.value() != null && timerEventTime.value().equals(timestamp - windowsTime.value())) {
            Integer validCount = clearOldEventAndRCalculateValidCount(timestamp);
            if (validCount == 0) {
                System.out.println("clear timerEventTime," + timestamp);
                windowCountState.clear();
                timerEventTime.clear();
            }
        } else {
            Result result = new Result(timestamp, ctx.getCurrentKey().f0, ruleId.value());
            System.out.println("send to mq" + JSON.toJSONString(result));
            out.collect(result);
        }

    }

    @Override
    public void processBroadcastElement(Rule value, Context ctx, Collector<Result> out) throws Exception {
        BroadcastState<String, Rule> broadcastState = ctx.getBroadcastState(BroadcastStateDescriptors.RULES_BROADCAST_STATE_DESCRIPTOR);
        broadcastState.put(String.valueOf(value.getId()), value); // 假设Rule有一个getId()方法返回其唯一标识符

    }

    private Integer clearOldEventAndRCalculateValidCount(long currentTime) throws Exception {
        Iterator<Long> windows = windowCountState.get().iterator();
        int count = 0;
        while (windows.hasNext()) {
            Long windowStart = windows.next();
            if (windowStart <= currentTime - windowsTime.value()) {
                windows.remove();
            } else {
                count++;
            }
        }
        return count;
    }

    private void clearNotifyTimer(Event event, ReadOnlyContext ctx) throws Exception {
        Iterator<Long> browseAchievements = browseAchievement.get().iterator();
        System.out.println("achievement purchase,event:" + event.toString());

        //clear all notify Timer
        while (browseAchievements.hasNext()) {
            long browseAchievementTime = browseAchievements.next();
            long expectedNotifyTime = browseAchievementTime + waitingTime.value();
            ctx.timerService().deleteProcessingTimeTimer(expectedNotifyTime);
            System.out.println("purchase clear achievement Timer," + expectedNotifyTime);
            browseAchievements.remove();
        }
        browseAchievement.clear();
    }
}
