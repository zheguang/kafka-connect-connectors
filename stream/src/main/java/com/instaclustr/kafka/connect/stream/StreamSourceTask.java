package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.codec.CharRecord;
import com.instaclustr.kafka.connect.stream.codec.CharDecoder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private Endpoint endpoint;
    private String filename;
    private String topic;
    private int batchSize;
    private Map<String, String> props;

    private CharDecoder decoder = null;

    // For Plugins to load this class
    public StreamSourceTask() {}

    @Override
    public String version() {
        return new StreamSourceConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        AbstractConfig connectorConfig = new AbstractConfig(StreamSourceConnector.CONFIG_DEF, props);

        endpoint = Endpoints.of(props);
        filename = connectorConfig.getString(StreamSourceConnector.FILE_CONFIG);
        topic = connectorConfig.getString(StreamSourceConnector.TOPIC_CONFIG);
        batchSize = connectorConfig.getInt(StreamSourceConnector.TASK_BATCH_SIZE_CONFIG);

        this.props = props;

        log.debug("Starting task reading from {} for topic {}", filename, topic);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (decoder == null) {
            try {
                InputStream inputStream = endpoint.openInputStream(filename);
                decoder = CharDecoder.of(inputStream, props);

                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
                log.debug("Read offset: {}, thread ID", offset);
                if (offset != null) {
                    Object lastRecordedOffset = offset.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordedOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        decoder.skipFirstBytes((long) lastRecordedOffset);
                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                }
                log.debug("Opened {} for reading", logFilename());
            } catch (IOException e) {
                log.error("Error while trying to open stream {}: ", filename, e);
                throw new ConnectException(e);
            }
        }

        try {
            List<CharRecord> charRecords = decoder.next(batchSize);
            List<SourceRecord> records = null;
            if (charRecords != null) {
                records = new ArrayList<>();
                for (var charRecord : charRecords) {
                    records.add(new SourceRecord(offsetKey(filename), offsetValue(charRecord.getStreamOffset()), topic, null,
                            null, null, VALUE_SCHEMA, charRecord.getRecord(), System.currentTimeMillis()));
                }
            } else {
                synchronized (this) {
                    this.wait(1000);
                }
            }
            log.debug("Return records after wait: size=" + (records == null ? null : records.size()));
            return records;

        } catch (IOException e) {
            log.debug("Error in reading records", e);
            log.warn("Underlying stream was killed, probably due to calling stop. Poll will return null");
        }
        return null;
    }

    @Override
    public void stop() {
        log.debug("Stopping");
        synchronized (this) {
            try {
                if (decoder != null) {
                    decoder.close();
                    log.debug("Closed stream reader");
                }
            } catch (IOException e) {
                log.error("Failed to close stream reader: ", e);
            }
            this.notify();
        }
    }

    private String logFilename() {
        return filename;
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }
}
