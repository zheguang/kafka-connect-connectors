package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.Primitive;
import org.apache.parquet.example.data.simple.BooleanValue;
import org.apache.parquet.example.data.simple.IntegerValue;
import org.apache.parquet.example.data.simple.LongValue;
import org.apache.parquet.example.data.simple.Int96Value;
import org.apache.parquet.example.data.simple.FloatValue;
import org.apache.parquet.example.data.simple.DoubleValue;
import org.apache.parquet.example.data.simple.BinaryValue;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;


public class ParquetKafkaDataConverter {

    private ParquetKafkaTypeConverter typeConverter;

    public static ParquetKafkaDataConverter newConverter() {
        ParquetKafkaTypeConverter typeConverter = new ParquetKafkaTypeConverter();
        return new ParquetKafkaDataConverter(typeConverter);
    }

    public ParquetKafkaDataConverter(ParquetKafkaTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    public Struct convert(SimpleGroup group) {
        assert group.getType() instanceof MessageType : "Unexpected Parquet root schema type: " + group.getType().getClass();
        Schema kafkaSchema = ((MessageType) group.getType()).convertWith(typeConverter);
        return convert(kafkaSchema, group);
    }

    public Struct convert(Schema kafkaSchema, SimpleGroup group) {
        Struct result = new Struct(kafkaSchema);
        int fieldIndex = 0;
        for (Type field : group.getType().getFields()) {
            final String name = field.getName();
            final int reps = group.getFieldRepetitionCount(fieldIndex);
            if (reps > 0) {
                for (int r = 0; r < reps; r++) {
                    Object value = group.getObject(fieldIndex, r);
                    Schema kafkaFieldSchema = kafkaSchema.field(name).schema();
                    if (value == null) {
                        result.put(name, null);
                    } else if (field instanceof GroupType) {
                        assert value instanceof SimpleGroup : "Unsupported Parquet group value type: " + value.getClass();
                        result.put(name, convert(kafkaFieldSchema, (SimpleGroup) value));
                    } else {
                        // Primitive
                        assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
                        switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                            case BINARY:
                            case INT96:
                            case FIXED_LEN_BYTE_ARRAY:
                                assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                                result.put(name, ((Binary) value).getBytes());
                                break;
                            default:
                                assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                                result.put(name, value);
                                break;
                        }
                    }
                }
            }
            ++fieldIndex;
        }
        return result;
    }
}
