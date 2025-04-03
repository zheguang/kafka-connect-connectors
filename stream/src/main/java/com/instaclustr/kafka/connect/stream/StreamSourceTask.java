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

import javax.xml.transform.Source;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class StreamSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(StreamSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final long POLL_WAIT_MS = 1000;
    public static final int MAX_TRIES = 10;

    private Endpoint endpoint;
    private Queue<String> filenames; // head of the queue is the current file
    private String topic;
    private int batchSize;
    private Map<String, String> props;

    private CharDecoder decoder = null;
    private int numTries = 0;

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
        filenames = new LinkedList<>(connectorConfig.getList(StreamSourceConnector.FILES_CONFIG));
        topic = connectorConfig.getString(StreamSourceConnector.TOPIC_CONFIG);
        batchSize = connectorConfig.getInt(StreamSourceConnector.TASK_BATCH_SIZE_CONFIG);

        // Set decoder to null so poll() opens a fresh stream
        decoder = null;

        this.props = props;

        log.debug("Starting task reading from {} for topic {}", filenames, topic);
    }

    /**
     * Poll the stream source for new records.
     * @return null if blocking for more source input; a nonempty list of records otherwise; never an empty list.
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Process a list of files one by one
        // For each file,
        // - set filename, topic, and its decoder (open stream and seek)
        // - reading until EOF or not ready
        // - when EOF or not ready, pause on this file by closing its stream, process next file
        // - keep looping forever over all files
        // - what happens when error on one file? remove this file from list, and move on to the next file
        if (decoder == null) {
            decoder = maybeGetNextFileDecoder();
            if (decoder == null) {
                log.error("No available file to read, will try again later");
                waitForThrottle();
                return null;
            }
        }

        try {
            String filename = filenames.element();
            List<CharRecord> charRecords = decoder.next(batchSize);
            numTries++;
            if (charRecords == null) {
                log.debug("Stream is not available to read, at try: {}, file: {}", numTries, filename);
                if (numTries >= MAX_TRIES) {
                    log.debug("Reached retry limit: {}, file: {}", numTries, filename);
                    closeForNextFile();
                }
                waitForThrottle();
                return null;
            }

            if (charRecords.isEmpty()) {
                log.debug("Read some new bytes from stream, but not enough yet to decode any record");
                // Not counting towards a retry
                return null;
            }

            List<SourceRecord> records = new ArrayList<>();
            for (var charRecord : charRecords) {
                records.add(new SourceRecord(
                        offsetKey(filename),
                        offsetValue(charRecord.getStreamOffset()),
                        topic,
                        null,
                        null,
                        null,
                        VALUE_SCHEMA,
                        charRecord.getRecord(),
                        System.currentTimeMillis())
                );
            }
            log.debug("Return records after decoding or waiting, size: {}, tries: {}", records.size(), numTries);
            numTries = 0; // Next try is first try
            // Non-empty result for Connect runtime to work on; otherwise null returns above indicate no result
            return records;
        } catch (IOException e) {
            log.debug("Error in reading records", e);
            log.warn("Underlying stream was killed, probably due to calling stop. Poll will return null");
            closeForNextFile();
            waitForThrottle();
        }
        return null;
    }

    private CharDecoder maybeGetNextFileDecoder() {
        log.debug("Looking for next file to read, total files: {}", filenames.size());
        CharDecoder result = null;
        for (int i = 0; i < filenames.size() && result == null; i++) {
            String filename = filenames.element();
            try {
                InputStream inputStream = endpoint.openInputStream(filename);
                result = CharDecoder.of(inputStream, props);

                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filenames));
                log.debug("Read offset: {}, thread ID", offset);
                if (offset != null) {
                    Object lastRecordedOffset = offset.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordedOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        result.skipFirstBytes((long) lastRecordedOffset);
                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                }
                log.debug("Opened {} for reading", filename);
            } catch (IOException e) {
                log.error("Error while trying to open stream {}: ", filename, e);
                closeForNextFile(); // Set decoder to null
            }
        }

        return result;
    }

    private void waitForThrottle() throws InterruptedException {
        synchronized (this) {
            log.debug("Waiting for throttle: {} ms", POLL_WAIT_MS);
            this.wait(POLL_WAIT_MS);
        }
    }

    private void closeForNextFile() {
        log.debug("Preparing for next file after closing file: {}", filenames.peek());
        // Move file to last in list to read, reset decoder
        maybeCloseDecoder();
        decoder = null;
        filenames.add(filenames.remove());
        numTries = 0;
    }

    @Override
    public void stop() {
        log.debug("Stopping");
        synchronized (this) {
            maybeCloseDecoder();
            // Don't set decoder to null, because prior to Kafka 2.8, stop() and poll() can be called
            // on separate threads, leading to race condition on decoder.
            // However, not setting decoder to null after stop() may lead to stale poll().
            // So, set decoder to null in start() to ensure restarting a task gets fresh stream.

            // Wake up any waiting poll() to finish
            // Can live with chance of having one waiting poll() AFTER stop
            this.notify();
        }
    }

    private void maybeCloseDecoder() {
        try {
            if (decoder != null) {
                decoder.close();
                log.debug("Closed stream reader");
            }
        } catch (IOException e) {
            log.error("Failed to close stream reader: ", e);
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }
}
