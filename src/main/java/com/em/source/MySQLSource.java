package com.em.source;

import com.em.pojo.Rule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MySQLSource extends RichSourceFunction<Rule> {

    private volatile boolean running = true;
    private List<Rule> rules = new ArrayList<>();

    // Configuration properties
    private String url;
    private String username;
    private String password;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Load configuration from file
        loadConfiguration();
        // Load data from MySQL table into memory
        loadDataFromMySQL();
    }

    @Override
    public void run(SourceContext<Rule> ctx) throws Exception {
        while (running) {
            // Sleep for 5 minutes
            // Load data from MySQL table into memory
            loadDataFromMySQL();
            // Emit the collected data
            for (Rule rule : rules) {
                ctx.collect(rule);
            }
            TimeUnit.MINUTES.sleep(5);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private void loadDataFromMySQL() throws SQLException {
        Connection conn = null;
        List<Rule> newRules = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(url, username, password);
            PreparedStatement statement = conn.prepareStatement("SELECT id, category, item_id, browse_times, windows_time, wait_time FROM rule");
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Rule rule = new Rule(
                            resultSet.getInt("id"),
                            resultSet.getString("category"),
                            resultSet.getInt("item_id"),
                            resultSet.getInt("browse_times"),
                            resultSet.getInt("windows_time"),
                            resultSet.getInt("wait_time")
                    );
                    newRules.add(rule);
                }
            }
            // Update the rules list with the latest data
            synchronized (this) {
                rules = newRules;
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    private void loadConfiguration() throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
            properties.load(inputStream);
            url = properties.getProperty("mysql.url");
            username = properties.getProperty("mysql.username");
            password = properties.getProperty("mysql.password");
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    public List<Rule> executeAndCollect() throws Exception {
        loadDataFromMySQL();
        return rules;
    }
}