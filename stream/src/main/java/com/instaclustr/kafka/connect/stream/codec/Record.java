package com.instaclustr.kafka.connect.stream.codec;

public interface Record<T> {

    T getRecord();

    long getStreamOffset();
}

