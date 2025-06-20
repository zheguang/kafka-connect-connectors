package com.instaclustr.kafka.connect.stream;

import static java.lang.Thread.sleep;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StreamSourceConnectorTest {
    private File dir;
    private Set<String> files;
    private Map<String, String> config;
    private StreamSourceConnector connector;
    
    @BeforeMethod
    public void setup() throws IOException {
        dir = Files.createTempDirectory("connector-test-method").toFile();
        dir.deleteOnExit();
        files = new HashSet<>();
        files.addAll(List.of(
                File.createTempFile("connector-test-file1", null, dir).getAbsolutePath(),
                File.createTempFile("connector-test-file2", null, dir).getAbsolutePath()));

        config = new HashMap<>();
        config.put(StreamSourceTask.TOPIC_CONFIG, "dummyTopic");
        config.put(Endpoints.ENDPOINT_TYPE, Endpoints.LOCAL_FILE);
        
        connector = new StreamSourceConnector();
    }
    
    @AfterMethod
    public void tearDown() {
        connector.stop();
    }
    
    @Test
    public void testDirectory() throws IOException {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        connector.start(config);

        assertEquals(Set.copyOf(connector.getFiles()), files);
    }
    
    @Test
    public void testFiles() throws IOException {
        config.put(StreamSourceConnector.FILES_CONFIG, String.join(",", files));
        connector.start(config);

        assertEquals(Set.copyOf(connector.getFiles()), files);
    }
    
    @Test(expectedExceptions = {ConnectException.class})
    public void shouldNotDefineBothDirectoryAndFiles() throws IOException {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        config.put(StreamSourceConnector.FILES_CONFIG, String.join(",", files));
        connector.start(config);
    }

    @Test(expectedExceptions = {ConnectException.class})
    public void shouldNotMissDirectoryAndFiles() throws IOException {
        connector.start(config);
    }
    
    @Test
    public void testFileDiscovery() throws IOException, InterruptedException {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        connector = Mockito.spy(connector);
        doReturn(Duration.ofSeconds(1)).when(connector).getDirectoryFileDiscoveryDuration();
        var context = mock(SourceConnectorContext.class);
        doReturn(context).when(connector).getContext();
        connector.start(config);
        assertEquals(Set.copyOf(connector.getFiles()), files);
        
        sleep(1500);
        verify(context, times(0)).requestTaskReconfiguration();
        files.add(File.createTempFile("file3", null, dir).getAbsolutePath());
        sleep(1000);;
        verify(context, times(1)).requestTaskReconfiguration();
        assertEquals(Set.copyOf(connector.getFiles()), files);
    }
    
    @Test
    public void testTaskConfigs() {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        connector.start(config);

        var tasks = connector.taskConfigs(1);
        assertEquals(tasks.size(), 1);
        
        var actualFiles = Arrays.asList(tasks.get(0).get(StreamSourceTask.TASK_FILES).split(","));
        assertEquals(Set.copyOf(actualFiles), files);
        
        tasks = connector.taskConfigs(123);
        assertEquals(tasks.size(), 1);
        actualFiles = Arrays.asList(tasks.get(0).get(StreamSourceTask.TASK_FILES).split(","));
        assertEquals(Set.copyOf(actualFiles), files);
    }
}
