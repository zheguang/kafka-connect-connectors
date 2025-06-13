package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.Paper;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

public class ParquetKafkaDataConverterTest {
    @Test
    public void convertGroup() {
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();

        Struct result = converter.convert(Paper.r1);

        ParquetKafkaTypeConverter typeConverter = new ParquetKafkaTypeConverter();
        assertTrue(Paper.r1.getType() instanceof MessageType);
        assertEquals(result.schema(), ((MessageType) Paper.r1.getType()).convertWith(typeConverter));
    }

    @Test
    public void convertSimpleGroup() {
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();

        // message Document {
        //     required int64 DocId;
        //     repeated group Name {
        //         repeated group Language {
        //             optional binary Country;
        //         }
        //     }
        // }
        //// r1
        // DocId: 10
        // Name
        //  Language
        //    Country: 'us'
        //  Language
        // Name
        // Name
        //  Language
        //    Country: 'gb'
        Struct result = converter.convert(Paper.pr1);
        assertEquals(result.get("DocId"), Paper.pr1.getObject("DocId", 0));
        assertTrue(result.get("Name") instanceof List);
        List<Struct> names = result.getArray("Name");
        assertEquals(names.size(), 3);

        // Document.Name[0].Language[0]
        List<Struct> languages =  names.get(0).getArray("Language");
        assertEquals(languages.size(), 2);
        assertEquals(languages.get(0).getBytes("Country"),
                Paper.pr1.getGroup("Name", 0)
                        .getGroup("Language", 0)
                        .getBinary("Country", 0).getBytes());

        // Document.Name[0].Language[1]
        assertNull(languages.get(1).get("Country"));

        // Document.Name[1]
        assertTrue(names.get(1).getArray("Language").isEmpty());

        // Document.Name[2].Language[0]
        languages = names.get(2).getArray("Language");
        assertEquals(languages.size(), 1);
        assertEquals(languages.get(0).getBytes("Country"),
                Paper.pr1.getGroup("Name", 2)
                        .getGroup("Language", 0)
                        .getBinary("Country", 0).getBytes());
    }
}
