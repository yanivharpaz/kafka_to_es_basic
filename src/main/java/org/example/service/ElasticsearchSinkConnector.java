package org.example.service;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSinkConnector extends SinkConnector {
    private Map<String, String> configProperties;
    private static final String VERSION = "1.0";

    @Override
    public void start(Map<String, String> props) {
        configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(configProperties));
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define("elasticsearch.host", ConfigDef.Type.STRING, "localhost", ConfigDef.Importance.HIGH, "Elasticsearch host")
            .define("elasticsearch.port", ConfigDef.Type.INT, 9200, ConfigDef.Importance.HIGH, "Elasticsearch port");
    }

    @Override
    public String version() {
        return VERSION;
    }
} 