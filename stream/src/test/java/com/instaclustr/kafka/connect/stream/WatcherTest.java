package com.instaclustr.kafka.connect.stream;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.http.util.Asserts;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WatcherTest {

    int count; 
    
    @BeforeMethod
    public void setup() {
        count = 0;
    }
    
    @Test
    public void testWatch() throws Exception {
        Callable<Boolean> condition = Mockito.mock(Callable.class);
        Mockito.when(condition.call()).thenReturn(true, false, true, false);
        Watcher watcher = Watcher.of(Duration.ofSeconds(1));
        watcher.watch(condition, () -> count++);
        Thread.sleep(Duration.ofSeconds(4));
        watcher.close();
        Assert.assertEquals(count, 2);
    }
}
