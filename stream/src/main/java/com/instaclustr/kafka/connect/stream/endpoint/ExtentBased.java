package com.instaclustr.kafka.connect.stream.endpoint;

import java.io.IOException;
import java.io.InputStream;

public interface ExtentBased {

    InputStream openInputStream(String filename, long start, long length) throws IOException;
}
