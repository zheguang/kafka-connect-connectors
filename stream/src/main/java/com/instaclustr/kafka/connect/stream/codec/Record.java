package com.instaclustr.kafka.connect.stream.codec;

public class Record<T> {

    private final T record;
    private final long streamOffset;

    public Record(T record, long streamOffset) {
        this.record = record;
        this.streamOffset = streamOffset;
    }

    public T getRecord() {
        return record;
    }

    public long getStreamOffset() {
        return streamOffset;
    }
}

