package com.em.source;

import com.em.pojo.Event;

import java.util.ArrayList;
import java.util.List;

public  class MockEventSource extends org.apache.flink.streaming.api.functions.source.RichSourceFunction<Event> {
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
//            {"timestamp": 1712180317679, "category": "Category A", "itemId": 1001, "userId": 1, "type": 1}

    }

    @Override
    public void cancel() {
        running = false;
    }
}