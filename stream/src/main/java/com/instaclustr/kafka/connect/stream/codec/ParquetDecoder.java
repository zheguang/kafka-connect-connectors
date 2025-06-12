package com.instaclustr.kafka.connect.stream.codec;

import com.instaclustr.kafka.connect.stream.RandomAccessInputStream;
import com.instaclustr.kafka.connect.stream.types.parquet.ParquetKafkaDataConverter;
import com.instaclustr.kafka.connect.stream.types.parquet.StreamInputFile;
import com.instaclustr.kafka.connect.stream.types.parquet.ParquetKafkaDataConverter;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.StreamParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;

public class ParquetDecoder implements Decoder<Record<Struct>> {

    private final StreamParquetReader reader;
    private final ParquetKafkaDataConverter converter;

    public ParquetDecoder(StreamParquetReader reader, ParquetKafkaDataConverter converter) {
        this.reader = reader;
        this.converter = converter;
    }

    public static ParquetDecoder from(RandomAccessInputStream rais) throws IOException {
        SeekableInputStream sis = new DelegatingSeekableInputStream(rais) {
            @Override
	    public long getPos() throws IOException {
                return rais.getStreamOffset();
            }

            @Override
	    public void seek(long newPos) throws IOException {
                rais.seek(newPos);
            }
        };
        StreamParquetReader reader = new StreamParquetReader(new StreamInputFile(() -> sis, rais.getSize()));
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        return new ParquetDecoder(reader, converter);
    }

    @Override
    public List<Record<Struct>> next(int batchSize) throws IOException {
        if (reader.getProgress() >= 1) {
            return null;
        }
        List<Record<Struct>> result = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            SimpleGroup group = (SimpleGroup) reader.read();
            if (group == null) {
                return result;
            }
            Struct struct = converter.convert(group);
            result.add(new Record<>(struct, 0)); // do not remember progress
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
