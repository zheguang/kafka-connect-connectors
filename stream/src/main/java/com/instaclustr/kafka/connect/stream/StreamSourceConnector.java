package com.instaclustr.kafka.connect.stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(StreamSourceConnector.class);
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILES_CONFIG = "files";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILES_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Source filenames.")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(TASK_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_TASK_BATCH_SIZE, ConfigDef.Importance.LOW,
                    "The maximum number of records the source task can read from the file each time it is polled");

    private Map<String, String> props;

    @Override
    public void start(final Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        List<String> filenames = config.getList(FILES_CONFIG);
        log.info("Start S3 source connector reading from {} for topic {}", filenames, config.getString(TOPIC_CONFIG));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stop");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        return ExactlyOnceSupport.SUPPORTED;
    }

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, connectorConfig);

        for (Map.Entry<Map<String, ?>, Map<String, ?>> partitionOffset : offsets.entrySet()) {
            Map<String, ?> offset = partitionOffset.getValue();
            if (offset == null) {
                continue;
            }

            if (!offset.containsKey(StreamSourceTask.POSITION_FIELD)) {
                throw new ConnectException("Offset objects should either be null or contain the key '" + StreamSourceTask.POSITION_FIELD + "'");
            }

            if (!(offset.get(StreamSourceTask.POSITION_FIELD) instanceof Long)) {
                throw new ConnectException("The value for the '" + StreamSourceTask.POSITION_FIELD + "' key in the offset is expected to be a Long value");
            }

            long offsetPosition = (Long) offset.get(StreamSourceTask.POSITION_FIELD);
            if (offsetPosition < 0) {
                throw new ConnectException("The value for the '" + StreamSourceTask.POSITION_FIELD + "' key in the offset should be a non-negative value");
            }

            Map<String, ?> partition = partitionOffset.getKey();
            if (partition == null) {
                throw new ConnectException("Partition objects cannot be null");
            }

            if (!partition.containsKey(StreamSourceTask.FILENAME_FIELD)) {
                throw new ConnectException("Partition objects should contain the key '" + StreamSourceTask.FILENAME_FIELD + "'");
            }
        }

        return true;
    }
}
