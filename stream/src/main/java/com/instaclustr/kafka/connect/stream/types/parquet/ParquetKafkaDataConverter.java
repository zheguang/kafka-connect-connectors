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

import java.util.ArrayList;
import java.util.List;


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
        System.err.println("\n-> Converting: " + kafkaSchema.name());
        // Parquet row, aka SimpleGroup:
        // - Its schema matches the present fields in the row
        // - Its fields are always lists, where empty list means null optional value, singleton list for present optional, required, or repeated-once value, multi-element list for repeated value
        // Kakf row, aka Struct:
        // - Its schema matches the present fields in the row, same as Parquet
        // - Its fields are NOT always lists: null means null optioanl value, non-collection object for present optional or required value, a list for repeated value
        // This difference is derived from the difference betweem the two type systems: List is reserved for Kafka's ARRAY type
        Struct result = new Struct(kafkaSchema);
        int fieldIndex = 0;
        for (Type field : group.getType().getFields()) {
            final String name = field.getName();
            final int reps = group.getFieldRepetitionCount(fieldIndex);
            switch (field.getRepetition()) {
                case REQUIRED: {
                    assert reps == 1 : "Required value should appear once, but found: " + reps;
                    Object kafkaValue = convertSingleton(kafkaSchema, group, field, fieldIndex);
                    System.err.println("Put: " + name + "=" + kafkaValue);
                    result.put(name, convertSingleton(kafkaSchema, group, field, fieldIndex));
                }
                break;

                case OPTIONAL: {
                    if (reps <= 0) {
                        // Treat Name.Language: null explicitly as a mapping in Struct, as its Field shows up in Parquet Schema
                        System.err.println("Put: " + name + "=null0");
                        result.put(name, null);
                    } else {
                        assert reps == 1 : "Optional value should be present no more than once, but found: " + reps;
                        Object kafkaValue = convertSingleton(kafkaSchema, group, field, fieldIndex);
                        System.err.println("Put: " + name + "=" + kafkaValue);
                        result.put(name, convertSingleton(kafkaSchema, group, field, fieldIndex));
                    }
                }
                break;

                case REPEATED: {
                    List<Object> values = convertArray(kafkaSchema, group, field, reps, fieldIndex);
                    result.put(name, values);
                }
                break;

                default:
                    throw new ConverterError("Unexpected repetition: " + field.getRepetition());
            }
            /*if (reps <= 0) {
                // Treat Name.Language: null explicitly as a mapping in Struct, as its Field shows up in Parquet Schema
                System.err.println("Put: " + name + "=null0");
                result.put(name, null);
            } else if (reps == 1) {
                // TODO: separate the case of repeated-once value (array) versus required or optional-present value
                Object value = group.getObject(fieldIndex, 0);
                if (value == null) {
                    System.err.println("Put: " + name + "=null1");
                    result.put(name, null);
                } else if (field instanceof GroupType) {
                    assert value instanceof SimpleGroup : "Unsupported Parquet group value type: " + value.getClass();
                    Schema kafkaFieldSchema = kafkaSchema.fields().get(fieldIndex).schema();
                    Struct kafkaValue = convert(kafkaFieldSchema, (SimpleGroup) value);
                    System.err.println("Put: " + name + "=" + kafkaValue);
                    result.put(name, kafkaValue);
                } else {
                    // Primitive
                    assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
                    switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                        case BINARY:
                        case INT96:
                        case FIXED_LEN_BYTE_ARRAY:
                            assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                            System.err.println("Put: " + name + "=" + value);
                            result.put(name, ((Binary) value).getBytes());
                            break;
                        default:
                            assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                            System.err.println("Put: " + name + "=" + value);
                            result.put(name, value);
                            break;
                    }
                }
            } else {
                List<Object> values = new ArrayList<>(reps);
                if (field instanceof GroupType) {
                    for (int r = 0; r < reps; r++) {
                        Object value = group.getObject(fieldIndex, r);
                        assert value instanceof SimpleGroup : "Array of structs";
                        Schema kafkaFieldSchema = kafkaSchema.fields().get(fieldIndex).schema().valueSchema();
                        values.add(convert(kafkaFieldSchema, (SimpleGroup) value));
                    }
                } else {
                    // Primitive
                    assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
                    switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                        case BINARY:
                        case INT96:
                        case FIXED_LEN_BYTE_ARRAY:
                            for (int r = 0; r < reps; r++) {
                                Object value = group.getObject(fieldIndex, r);
                                assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                                values.add(((Binary) value).getBytes());
                            }
                            break;
                        default:
                            for (int r = 0; r < reps; r++) {
                                Object value = group.getObject(fieldIndex, r);
                                assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                                values.add(value);
                            }
                            break;
                    }
                }
                result.put(name, values);
            }*/
            ++fieldIndex;
        }
        System.err.println("<- Done: " + kafkaSchema.name());
        return result;
    }

    private List<Object> convertArray(final Schema kafkaSchema, final SimpleGroup group, final Type field, final int reps, final int fieldIndex) {
        List<Object> values = new ArrayList<>(reps);
        if (field instanceof GroupType) {
            for (int r = 0; r < reps; r++) {
                Object value = group.getObject(fieldIndex, r);
                assert value instanceof SimpleGroup : "Array of structs";
                Schema kafkaFieldSchema = kafkaSchema.fields().get(fieldIndex).schema().valueSchema();
                values.add(convert(kafkaFieldSchema, (SimpleGroup) value));
            }
        } else {
            // Primitive
            assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
            switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                case BINARY:
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                    for (int r = 0; r < reps; r++) {
                        Object value = group.getObject(fieldIndex, r);
                        assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                        values.add(((Binary) value).getBytes());
                    }
                    break;
                default:
                    for (int r = 0; r < reps; r++) {
                        Object value = group.getObject(fieldIndex, r);
                        assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                        values.add(value);
                    }
                    break;
            }
        }
        return values;
    }

    private Object convertSingleton(final Schema kafkaSchema, final SimpleGroup group, final Type field, final int fieldIndex) {
        Object value = group.getObject(fieldIndex, 0);
        assert value != null : "Required value should be nonnull";
        Object kafkaValue;
        if (field instanceof GroupType) {
            assert value instanceof SimpleGroup : "Unsupported Parquet group value type: " + value.getClass();
            Schema kafkaFieldSchema = kafkaSchema.fields().get(fieldIndex).schema();
            kafkaValue = convert(kafkaFieldSchema, (SimpleGroup) value);
        } else {
            // Primitive
            assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
            switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                case BINARY:
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                    assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                    kafkaValue = ((Binary) value).getBytes();
                    break;
                default:
                    assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                    kafkaValue = value;
                    break;
            }
        }
        assert kafkaValue != null;
        return kafkaValue;
    }
}
