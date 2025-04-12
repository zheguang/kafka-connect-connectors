package com.instaclustr.kafka.connect.stream;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Watcher implements Closeable {
	private static final Logger log = LoggerFactory.getLogger(Watcher.class);
	
	private final Duration checkDelay;
	private final Duration stopDelay;
	private final Duration timeout;
	private final ScheduledExecutorService scheduler;
	private final ExecutorService executor;
	
	Watcher(Duration checkDelay, Duration stopDelay, Duration timeout, ScheduledExecutorService scheduler, ExecutorService executor) {
	    this.checkDelay = checkDelay;
	    this.stopDelay = stopDelay;
	    this.timeout = timeout;
	    this.scheduler = scheduler;
	    this.executor = executor;
	}
	
	public static Watcher of(Duration checkDelay) {
	    return new Watcher(checkDelay, Duration.ofSeconds(5), Duration.ofMinutes(1), Executors.newScheduledThreadPool(2), Executors.newFixedThreadPool(2));
	}

	public void watch(Callable<Boolean> condition, Runnable action) {
	    
	    scheduler.scheduleAtFixedRate(() -> {
	        var future = executor.submit(() -> {
                try {
                    if (condition.call()) {
                        log.debug("Condition met, running action, thread: {}", Thread.currentThread().getName());
                        action.run();
                    } else {
                        log.debug("Condition not met, thread: {}", Thread.currentThread().getName());
                    }
                } catch (Exception e) {
                    log.error("Error in watching condition: ", e);
                }
	        });
	        try {
                future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                log.warn("Unable to wait for condition to meet, canceling the action", e);
                future.cancel(true);
            }
	    }, checkDelay.getSeconds(), checkDelay.getSeconds(), TimeUnit.SECONDS);
	}

	@Override
	public void close() throws IOException {
		scheduler.shutdown();
		try {
			if (! scheduler.awaitTermination(stopDelay.getSeconds(), TimeUnit.SECONDS)) {
				scheduler.shutdownNow();
				if (! scheduler.awaitTermination(stopDelay.getSeconds(), TimeUnit.SECONDS)) {
					log.error("Thread pool did not terminate");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)cancel if interrupted above
			scheduler.shutdownNow();
			// Propagate interrupt
			Thread.currentThread().interrupt();
		}
	}

}
