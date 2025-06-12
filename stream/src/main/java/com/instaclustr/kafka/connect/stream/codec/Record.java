package com.instaclustr.kafka.connect.stream.codec;

public class Record<T> {

    private final T record;
    private final long streamOffset;

    public Record(T record, long streamOffset) {
        assert streamOffset >= 0 : "expect nonnegative stream offset";
        this.record = record;
        this.streamOffset = streamOffset;
    }

    public T getRecord() {
        return record;
    }

    /**
     * Offset for the next record position, logical to the source system.
     *
     * @return null if not using stream offset; else a nonnegative number.
     */
    public long getStreamOffset() {
        return streamOffset;
    }
}

