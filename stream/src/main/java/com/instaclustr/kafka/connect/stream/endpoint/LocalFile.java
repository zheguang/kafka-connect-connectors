package com.instaclustr.kafka.connect.stream.endpoint;

import com.instaclustr.kafka.connect.stream.Endpoint;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalFile implements Endpoint {
    @Override
    public InputStream openInputStream(final String streamName) throws IOException {
        return Files.newInputStream(Path.of(streamName));
    }

    @Override
    public Stream<String> listRegularFiles(final String path) throws IOException {
        File f = new File(path);
        Stream<String> result;
        if (f.isDirectory()) {
            var fs = f.listFiles(File::isFile);
            if (fs == null) {
                result = Stream.empty();
            } else {
                result = Arrays.stream(fs).map(File::getAbsolutePath);
            }
        } else if (f.isFile()) {
            result = Stream.of(f.getAbsolutePath());
        } else {
            throw new IOException("Path is neither a directory nor a file: " + path);
        }
        return result;
    }
}
