package com.instaclustr.kafka.connect.stream.endpoint;

import com.instaclustr.kafka.connect.stream.Endpoint;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalFile implements Endpoint {
    @Override
    public InputStream openInputStream(final String streamName) throws IOException {
        return Files.newInputStream(Path.of(streamName));
    }
}
