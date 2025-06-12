package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.Paper;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ParquetKafkaDataConverterTest {
    @Test
    public void convertGroup() {
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();

        Struct result = converter.convert(Paper.r1);

        ParquetKafkaTypeConverter typeConverter = new ParquetKafkaTypeConverter();
        assertTrue(Paper.r1.getType() instanceof MessageType);
        assertEquals(result.schema(), ((MessageType) Paper.r1.getType()).convertWith(typeConverter));
    }
}
