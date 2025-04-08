package com.instaclustr.kafka.connect.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;

public interface Endpoint {

    InputStream openInputStream(String streamName) throws IOException;

    Stream<String> listRegularFiles(String path) throws IOException;
}
