//package com.em.transform;
//
//import com.em.pojo.Event;
//import com.em.pojo.Rule;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.util.Collector;
//
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//
//import java.util.List;
//
//public class CountCategoryItem extends RichFlatMapFunction<Event, Tuple2<String, Integer>> {
//    private MapState<String, Integer> userCategoryItemCountState;
//    private MapState<String, Long> windowStartTimeState;
//    private List<Rule> rules;
//
//    public CountCategoryItem(List<Rule> rules) {
//        this.rules = rules;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        // Initialize the state
//        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>(
//                "userCategoryItemCount",
//                String.class,
//                Integer.class
//        );
//        userCategoryItemCountState = getRuntimeContext().getMapState(descriptor);
//
//        // Initialize the window start time state
//        MapStateDescriptor<String, Long> windowStartTimeDescriptor = new MapStateDescriptor<>(
//                "windowStartTime",
//                String.class,
//                Long.class
//        );
//        windowStartTimeState = getRuntimeContext().getMapState(windowStartTimeDescriptor);
//    }
//
//    @Override
//    public void flatMap(Event event, Collector<Tuple2<String, Integer>> out) throws Exception {
//        // Get the category and item ID from the event
//        String category = event.getCategory();
//        String itemId = event.getItemId();
//
//        // Concatenate userId, category, and itemId with '|' separator
//        String key = event.getUserId() + "|" + category + "|" + itemId;
//
//        // Iterate through the list of rules to find a match
//        for (Rule rule : rules) {
//            if (rule.getCategory().equals(category) && rule.getItemId() == Integer.parseInt(itemId)) {
//                // Get the window start time for the current key
//                Long windowStartTime = windowStartTimeState.contains(key) ? windowStartTimeState.get(key) : null;
//                if (windowStartTime == null) {
//                    windowStartTime = event.getTimestamp();
//                    windowStartTimeState.put(key, windowStartTime);
//                }
//
//                // Calculate the sliding window end time
//                long windowEndTime = windowStartTime + rule.getWindowsTime() * 1000;
//
//                // Match found, update the user's browse count if within the sliding window
//                if (event.getTimestamp() <= windowEndTime) {
//                    int count = userCategoryItemCountState.contains(key) ? userCategoryItemCountState.get(key) : 0;
//                    userCategoryItemCountState.put(key, count + 1);
//
//                    // If the browse count exceeds the threshold, emit the result
//                    if (count + 1 >= rule.getBrowseTimes()) {
//                        out.collect(new Tuple2<>(event.getUserId(), rule.getId()));
//                    }
//                }
//
//                // Remove expired sliding windows
//                if (windowEndTime < event.getTimestamp()) {
//                    userCategoryItemCountState.remove(key);
//                    windowStartTimeState.remove(key);
//                }
//
//                // Return after finding a match, no need to continue iterating through rules
//                return;
//            }
//        }
//    }
//}
