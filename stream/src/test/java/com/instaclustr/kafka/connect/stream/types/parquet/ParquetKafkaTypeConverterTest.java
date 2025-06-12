package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.parquet.example.Paper;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ParquetKafkaTypeConverterTest {
    
    @Test
    public void nestedType() {
        final ParquetKafkaTypeConverter converter = new ParquetKafkaTypeConverter();

        final Schema kafkaType = Paper.schema.convertWith(converter);

        assertEquals(kafkaType.name(), "Document");
        assertEquals(kafkaType.fields().size(), 3);

        // DocId
        final Field docId = kafkaType.fields().get(0);
        checkTypeName(docId, "DocId");
        
        // Links
        final Field links = kafkaType.fields().get(1);
        checkTypeName(links, "Links");

        final Field backward  = links.schema().fields().get(0);
        checkTypeName(backward, "Backward");

        final Field forward = links.schema().fields().get(1);
        checkTypeName(forward, "Forward");

        // Name
        final Field name = kafkaType.fields().get(2);
        checkTypeName(name, "Name");
        assertEquals(name.schema().fields().size(), 2);

        final Field language = name.schema().fields().get(0);
        checkTypeName(language, "Language");
        assertEquals(language.schema().fields().size(), 2);

        final Field url = name.schema().fields().get(1);
        checkTypeName(url, "Url");

        final Field code = language.schema().fields().get(0);
        checkTypeName(code, "Code");

        final Field country = language.schema().fields().get(1);
        checkTypeName(country, "Country");
    }

    @Test
    public void primitiveType() {

    }

    private static void checkTypeName(Field field, String name) {
        assertEquals(field.name(), name);
        assertEquals(field.schema().name(), name);
    }
}
