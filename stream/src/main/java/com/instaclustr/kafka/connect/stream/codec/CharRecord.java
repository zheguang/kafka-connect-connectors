package com.instaclustr.kafka.connect.stream.codec;

public class CharRecord implements Record<String> {
    private final String record;
    private final long streamOffset;

    public CharRecord(String record, long streamOffset) {
        this.record = record;
        this.streamOffset = streamOffset;
    }

    @Override
    public String getRecord() {
        return record;
    }

    @Override
    public long getStreamOffset() {
        return streamOffset;
    }
}
