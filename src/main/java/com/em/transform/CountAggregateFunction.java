package com.em.transform;

import lombok.Getter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import com.em.pojo.Event;

import java.util.ArrayList;
import java.util.List;

@Getter
public class CountAggregateFunction implements AggregateFunction<Event, Long, Tuple2<Long, String>> {

    private final List<Event> exceedingEvents;

    public CountAggregateFunction() {
        this.exceedingEvents = new ArrayList<>();
    }

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event event, Long accumulator) {
        long newCount = accumulator + 1;
        if (newCount > 3) {
            exceedingEvents.add(event);
        }
        return newCount;
    }

    @Override
    public Tuple2<Long, String> getResult(Long accumulator) {
        String resultMessage = accumulator > 3 ? "Count exceeds threshold" : "Count does not exceed threshold";
        return Tuple2.of(accumulator, resultMessage);
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }

}
