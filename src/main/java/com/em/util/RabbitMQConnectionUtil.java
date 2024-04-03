package com.em.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class RabbitMQConnectionUtil {

    public static Connection createConnection() throws IOException, TimeoutException {
        Properties properties = new Properties();
        try (InputStream input = RabbitMQConnectionUtil.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading RabbitMQ configuration", e);
        }

        String host = properties.getProperty("rabbitmq.host");
        int port = Integer.parseInt(properties.getProperty("rabbitmq.port"));
        String username = properties.getProperty("rabbitmq.username");
        String password = properties.getProperty("rabbitmq.password");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);

        return factory.newConnection();
    }

    // 创建一个新的Channel
    public static Channel createChannel(Connection connection) throws IOException {
        return connection.createChannel();
    }
}
