package com.instaclustr.kafka.connect.stream.codec;

public class CharRecord {
    private final String record;
    private final long streamOffset;

    public CharRecord(String record, long streamOffset) {
        this.record = record;
        this.streamOffset = streamOffset;
    }

    public String getRecord() {
        return record;
    }

    public long getStreamOffset() {
        return streamOffset;
    }
}
