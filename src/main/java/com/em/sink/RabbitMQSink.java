package com.em.sink;

import com.alibaba.fastjson.JSON;
import com.em.util.RabbitMQConnectionUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class RabbitMQSink<T> extends RichSinkFunction<T> {
    private transient Channel channel;
    private transient Connection connection;
    private String queueName = "flink-result"; // 可以通过构造器传递

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = RabbitMQConnectionUtil.createConnection();
        channel = RabbitMQConnectionUtil.createChannel(connection);
        // Ensure the queue exists
        channel.queueDeclare(queueName, true, false, false, null);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        String message = JSON.toJSONString(value); // Assuming T can be safely converted to String
        channel.basicPublish("", queueName, null, message.getBytes());
    }

    @Override
    public void close() throws Exception {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
