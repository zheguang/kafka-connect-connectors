package com.instaclustr.kafka.connect.stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(StreamSourceConnector.class);
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILES_CONFIG = "files";
    public static final String DIRECTORY_CONFIG = "directory";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
    public static final String READ_RETRIES = "read.retries";
    public static final String POLL_THROTTLE_MS = "poll.throttle.ms";
    public static final String DIRECTORY_FILE_DISCOVERY_MINUTES = "directory.file.discovery.minutes";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;
    public static final int DEFAULT_READ_RETRIES = 10;
    public static final long DEFAULT_POLL_THROTTLE_MS = 1000;
    public static final long DEFAULT_DIRECTORY_FILE_DISCOVERY_MINUTES = 60;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILES_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, "Source filenames, default to files under the same directory.")
            .define(DIRECTORY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Source directory.")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(TASK_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_TASK_BATCH_SIZE, ConfigDef.Importance.LOW,
                    "The maximum number of records the source task can read from the file each time it is polled")
            .define(READ_RETRIES, ConfigDef.Type.INT, DEFAULT_READ_RETRIES, ConfigDef.Importance.LOW, "The maximum number of retries on reading a stream")
            .define(POLL_THROTTLE_MS, ConfigDef.Type.LONG, DEFAULT_POLL_THROTTLE_MS, ConfigDef.Importance.LOW, "The time to wait for throttle source polling")
            .define(DIRECTORY_FILE_DISCOVERY_MINUTES, ConfigDef.Type.LONG, DEFAULT_DIRECTORY_FILE_DISCOVERY_MINUTES, ConfigDef.Importance.LOW, "The time to discover new directory files");

    private Map<String, String> props;
    private List<String> files;
    private Watcher directoryWatcher = null;

    @Override
    public void start(final Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        mustDefineDirectoryXorFiles(config);
        if ((files = config.getList(FILES_CONFIG)) == null) {
            Endpoint endpoint = Endpoints.of(props);
            String directory = config.getString(DIRECTORY_CONFIG);
            try {
                files = endpoint.listRegularFiles(directory).collect(Collectors.toList());
            } catch (IOException e) {
                throw new ConnectException(e);
            }
            directoryWatcher = Watcher.of(getDirectoryFileDiscoveryDuration());
            directoryWatcher.watch(() -> {
                var knownFiles = Set.copyOf(files);
                return ! endpoint.listRegularFiles(directory).allMatch(knownFiles::contains);
            }, () -> {
                log.info("Files under the directory has changed, request to reconfigure tasks");
                getContext().requestTaskReconfiguration();
            });
        }
        if (files.isEmpty()) {
            throw new ConnectException("Unable to find files to read, files: " + config.getString(FILES_CONFIG) + ", directory: " + config.getString(DIRECTORY_CONFIG));
        }
        log.info("Start stream source connector reading from {} for topic {}", files, config.getString(TOPIC_CONFIG));

    }

    private void mustDefineDirectoryXorFiles(AbstractConfig config) {
        if (! (config.getString(DIRECTORY_CONFIG) == null ^ config.getList(FILES_CONFIG) == null)) {
            throw new ConnectException("Files or directory should be configured exclusively");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        props.put(FILES_CONFIG, String.join(",", files));
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stop");
        if (directoryWatcher != null) {
            try {
                directoryWatcher.close();
            } catch (IOException e) {
                throw new ConnectException("Error while closing directory watcher threads", e);
            } finally {
                directoryWatcher = null;
            }
        }
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
    
    // Visible for testing
    List<String> getFiles() {
        return files;
    }
    
    Duration getDirectoryFileDiscoveryDuration() {
        return Duration.ofMinutes(Long.valueOf(props.get(DIRECTORY_FILE_DISCOVERY_MINUTES)));
    }
    
    SourceConnectorContext getContext() {
        return context();
    }
}
