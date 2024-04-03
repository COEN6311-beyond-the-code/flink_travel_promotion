package com.em.source;

import com.alibaba.fastjson.JSON;
import com.em.pojo.Event;
import com.em.util.RabbitMQConnectionUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQSource extends RichSourceFunction<Event> { // 注意这里改为Event
    private transient Connection connection;
    private transient Channel channel;
    private final String queueName = "flink-source";
    private volatile boolean running = true;

    public RabbitMQSource() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            connection = RabbitMQConnectionUtil.createConnection();
            channel = connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to connect to RabbitMQ", e);
        }
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            // 将接收到的消息从JSON字符串反序列化为Event对象
            Event event = JSON.parseObject(message, Event.class);
            event.setTimestamp(System.currentTimeMillis());
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(event); // 收集Event对象而不是字符串
            }
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                // Log and ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                // Log and ignore
            }
        }
    }
}

