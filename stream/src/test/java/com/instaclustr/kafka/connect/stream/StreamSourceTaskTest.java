package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.codec.CharDecoder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class StreamSourceTaskTest {

    private static final String TOPIC = "test";
    private static final int MAX_READ_RETRIES = 3;

    private File tempFile;
    private File tempFile2;
    private Map<String, String> config;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private StreamSourceTask task;

    @BeforeMethod
    public void setup() throws IOException {
        tempFile = File.createTempFile("file-stream-source-task-test", null);
        tempFile2 = File.createTempFile("file2-stream-source-task-test", null);
        config = new HashMap<>();
        config.put(Endpoints.ENDPOINT_TYPE, Endpoints.LOCAL_FILE);
        config.put(StreamSourceTask.TASK_FILES, tempFile.getAbsolutePath() + "," + tempFile2.getAbsolutePath());
        config.put(StreamSourceTask.TOPIC_CONFIG, TOPIC);
        config.put(StreamSourceTask.TASK_BATCH_SIZE_CONFIG,
                String.valueOf(StreamSourceTask.DEFAULT_TASK_BATCH_SIZE));
        config.put(StreamSourceTask.READ_RETRIES, String.valueOf(MAX_READ_RETRIES));
        config.put(StreamSourceTask.POLL_THROTTLE_MS, String.valueOf(10));
        config.put(CharDecoder.CHARACTER_SET, StandardCharsets.UTF_8.name());
        task = new StreamSourceTask();
        offsetStorageReader = mock(OffsetStorageReader.class);
        context = mock(SourceTaskContext.class);
        task.initialize(context);
    }

    @AfterMethod
    public void teardown() throws IOException {
        Files.deleteIfExists(tempFile.toPath());
    }

    @Test
    public void testNormalLifecycle() throws InterruptedException, IOException {
        expectOffsetLookupReturnNone();

        task.start(config);

        OutputStream os = Files.newOutputStream(tempFile.toPath());
        assertNull(task.poll());
        os.write("partial line".getBytes());
        os.flush();
        assertNull(task.poll());
        os.write(" finished\n".getBytes());
        os.flush();
        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(TOPIC, records.get(0).topic());
        assertEquals("partial line finished", records.get(0).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()),
                records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 22L), records.get(0).sourceOffset());
        assertNull(task.poll());

        // Different line endings, and make sure the final \r doesn't result in a line
        // until we can
        // read the subsequent byte.
        os.write("line1\rline2\r\nline3\nline4\n\r".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(4, records.size());
        assertEquals("line1", records.get(0).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()),
                records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 28L), records.get(0).sourceOffset());
        assertEquals("line2", records.get(1).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()),
                records.get(1).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 35L), records.get(1).sourceOffset());
        assertEquals("line3", records.get(2).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()),
                records.get(2).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 41L), records.get(2).sourceOffset());
        assertEquals("line4", records.get(3).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()),
                records.get(3).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 47L), records.get(3).sourceOffset());

        os.write("subsequent text".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(1, records.size());
        assertEquals("", records.get(0).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()),
                records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 48L), records.get(0).sourceOffset());

        // No more stream to read
        records = task.poll();
        assertNull(records);

        os.close();
        task.stop();

        verifyAll();
    }

    @Test
    public void testTwoFiles() throws IOException, InterruptedException {
        expectOffsetLookupReturnNone();
        task.start(config);
        Endpoint endpoint = spy(Endpoints.of(config));
        task.setEndpoint(endpoint);

        OutputStream os = Files.newOutputStream(tempFile.toPath());
        os.write("file1 line1\n".getBytes());
        os.write("file1 line2 partial".getBytes());
        os.flush();

        // read file1
        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(records.size(), 1);
        assertEquals(records.get(0).value(), "file1 line1");
        verifyOpenStreamCallsThenClear(endpoint, 1, 0);

        // set up file2
        OutputStream os2 = Files.newOutputStream(tempFile2.toPath());
        os2.write("file2 line1\n".getBytes());
        os2.write("file2 line2\n".getBytes());
        os2.write("file2 line3 partial".getBytes());
        os2.flush();

        // file1 EOF, reties
        for (int i = 0; i < 1 + MAX_READ_RETRIES; i++) {
            records = task.poll();
            assertNull(records);
            verifyOpenStreamCallsThenClear(endpoint, 0, 0);
        }

        // file1 close, file2 read
        expectOffsetLookupReturnNone();
        records = task.poll();
        assertNotNull(records);
        assertEquals(records.size(), 2);
        assertEquals(records.get(0).value(), "file2 line1");
        assertEquals(records.get(1).value(), "file2 line2");
        verifyOpenStreamCallsThenClear(endpoint, 0, 1);

        // file2 EOF
        records = task.poll();
        assertNull(records);
        verifyOpenStreamCallsThenClear(endpoint, 0, 0);

        // file2 retries
        for (int i = 0; i < MAX_READ_RETRIES; i++) {
            records = task.poll();
            assertNull(records);
            verifyOpenStreamCallsThenClear(endpoint, 0, 0);
        }

        // file2 close, file1 read EOF
        expectOffsetLookupReturnOffset("file1 line1\n".length());
        records = task.poll();
        assertNull(records);
        verifyOpenStreamCallsThenClear(endpoint, 1, 0);

        // file1 retry read appended result
        os.write(" complete\n".getBytes());
        os.flush();
        records = task.poll();
        assertNotNull(records);
        assertEquals(records.size(), 1);
        assertEquals(records.get(0).value(), "file1 line2 partial complete");
        verifyOpenStreamCallsThenClear(endpoint, 0, 0);

        os.close();
        os2.close();
        task.stop();
    }

    @Test
    public void testRestart() throws IOException, InterruptedException {
        expectOffsetLookupReturnNone();
        task.start(config);
        Endpoint endpoint = spy(Endpoints.of(config));
        task.setEndpoint(endpoint);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write("file1 line1\n".getBytes());
            os.write("file1 line2 partial".getBytes());
            os.flush();

            List<SourceRecord> records = task.poll();
            assertNotNull(records);
            assertEquals(records.size(), 1);
            assertEquals(records.get(0).value(), "file1 line1");
            verifyOpenStreamCallsThenClear(endpoint, 1, 0);

            // stop
            task.stop();

            // file append
            os.write(" complete\n".getBytes());
            os.flush();

            // restart
            expectOffsetLookupReturnOffset("file1 line1\n".length());
            task.start(config);
            task.setEndpoint(endpoint);

            records = task.poll();
            assertNotNull(records);
            assertEquals(records.size(), 1);
            assertEquals(records.get(0).value(), "file1 line2 partial complete");
            verifyOpenStreamCallsThenClear(endpoint, 1, 0);

            // stop, reconfigure to other files, start
            task.stop();
            config.put(StreamSourceTask.TASK_FILES, "bogusfilename,bogusfilename2");
            expectOffsetLookupReturnNone();
            task.start(config);
            task.setEndpoint(endpoint);
            records = task.poll();
            verify(endpoint, times(1)).openInputStream("bogusfilename");
            verify(endpoint, times(1)).openInputStream("bogusfilename2");
            verifyOpenStreamCallsThenClear(endpoint, 0, 0);
        }
    }

    @Test
    public void testRestartOnEndOfFile() throws IOException, InterruptedException {
        task.start(config);
        Endpoint endpoint = spy(Endpoints.of(config));
        task.setEndpoint(endpoint);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write("file1 line1\n".getBytes());
            os.flush();

            expectOffsetLookupReturnNone();
            List<SourceRecord> records = task.poll();
            assertNotNull(records);
            assertEquals(records.size(), 1);
            assertEquals(records.get(0).value(), "file1 line1");
            verifyOpenStreamCallsThenClear(endpoint, 1, 0);

            // restart
            task.start(config);
            task.setEndpoint(endpoint);
            expectOffsetLookupReturnOffsets((long) "file1 line1\n".length(), null);
            records = task.poll();
            verify(endpoint, times(1)).getFileSize(tempFile.getAbsolutePath());
            verify(endpoint, times(0)).getFileSize(tempFile2.getAbsolutePath());
            verifyOpenStreamCallsThenClear(endpoint, 0, 1);
        }
    }

    @Test
    public void testInvalidFile() throws InterruptedException, IOException {
        expectOffsetLookupReturnNone();
        // should continue processing, but log error
        config.put(StreamSourceTask.TASK_FILES, "bogusfilename,bogusfilename2");
        task.start(config);

        Endpoint endpoint = spy(Endpoints.of(config));
        task.setEndpoint(endpoint);

        for (int i = 1; i <= MAX_READ_RETRIES + 1; i++) {
            var res = task.poll();
            assertNull(res);
            verify(endpoint, times(1)).openInputStream("bogusfilename");
            verify(endpoint, times(1)).openInputStream("bogusfilename2");
            clearInvocations(endpoint);
        }
    }

    private void verifyOpenStreamCallsThenClear(Endpoint endpoint, int file1Calls, int file2Calls) throws IOException {
        verify(endpoint, times(file1Calls)).openInputStream(tempFile.getAbsolutePath());
        verify(endpoint, times(file2Calls)).openInputStream(tempFile2.getAbsolutePath());
        clearInvocations(endpoint);
    }

    private void expectOffsetLookupReturnNone() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);
    }

    private void expectOffsetLookupReturnOffset(long offset) {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap()))
                .thenReturn(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, offset));
    }

    private void expectOffsetLookupReturnOffsets(Long offset1, Long offset2) {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap()))
                .thenReturn(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, offset1))
                .thenReturn(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, offset2));
    }

    private void verifyAll() {
        verify(context).offsetStorageReader();
        verify(offsetStorageReader).offset(anyMap());
    }
}
