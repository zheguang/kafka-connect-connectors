package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.codec.CharDecoder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class StreamSourceTaskTest {

    private static final String TOPIC = "test";

    private File tempFile;
    private Map<String, String> config;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private StreamSourceTask task;

    @BeforeMethod
    public void setup() throws IOException {
        tempFile = File.createTempFile("file-stream-source-task-test", null);
        config = new HashMap<>();
        config.put(Endpoints.ENDPOINT_TYPE, Endpoints.LOCAL_FILE);
        config.put(StreamSourceConnector.FILE_CONFIG, tempFile.getAbsolutePath());
        config.put(StreamSourceConnector.TOPIC_CONFIG, TOPIC);
        config.put(StreamSourceConnector.TASK_BATCH_SIZE_CONFIG, String.valueOf(StreamSourceConnector.DEFAULT_TASK_BATCH_SIZE));
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
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 22L), records.get(0).sourceOffset());
        assertNull(task.poll());

        // Different line endings, and make sure the final \r doesn't result in a line until we can
        // read the subsequent byte.
        os.write("line1\rline2\r\nline3\nline4\n\r".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(4, records.size());
        assertEquals("line1", records.get(0).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 28L), records.get(0).sourceOffset());
        assertEquals("line2", records.get(1).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(1).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 35L), records.get(1).sourceOffset());
        assertEquals("line3", records.get(2).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(2).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 41L), records.get(2).sourceOffset());
        assertEquals("line4", records.get(3).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(3).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 47L), records.get(3).sourceOffset());

        os.write("subsequent text".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(1, records.size());
        assertEquals("", records.get(0).value());
        assertEquals(Collections.singletonMap(StreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(StreamSourceTask.POSITION_FIELD, 48L), records.get(0).sourceOffset());

        os.close();
        task.stop();

        verifyAll();
    }

    @Test(expectedExceptions = ConnectException.class)
    public void testInvalidFile() throws InterruptedException {
        config.put(StreamSourceConnector.FILE_CONFIG, "bogusfilename");
        task.start(config);
        for (int i = 0; i < 3; i++)
            task.poll(); // should throw error
    }

    private void expectOffsetLookupReturnNone() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);
    }

    private void verifyAll() {
        verify(context).offsetStorageReader();
        verify(offsetStorageReader).offset(anyMap());
    }
}
