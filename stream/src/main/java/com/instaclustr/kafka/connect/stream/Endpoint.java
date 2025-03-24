package com.instaclustr.kafka.connect.stream;

import java.io.IOException;
import java.io.InputStream;

public interface Endpoint {

    InputStream openInputStream(String streamName) throws IOException;
}
