package com.instaclustr.kafka.connect.stream.types.parquet;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.TypeConverter;

public class ParquetKafkaTypeConverter implements TypeConverter<Schema> {

    private static final Map<PrimitiveTypeName, Schema.Type> P2K_PRIMITIVE_TYPE = Map.of(
        PrimitiveTypeName.INT64,                Schema.Type.INT64,
        PrimitiveTypeName.INT32,                Schema.Type.INT32,
        PrimitiveTypeName.BOOLEAN,              Schema.Type.BOOLEAN,
        PrimitiveTypeName.BINARY,               Schema.Type.BYTES,
        PrimitiveTypeName.FLOAT,                Schema.Type.FLOAT32,
        PrimitiveTypeName.DOUBLE,               Schema.Type.FLOAT64,
        PrimitiveTypeName.INT96,                Schema.Type.BYTES,
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Schema.Type.BYTES
    );

    @Override
    public Schema convertPrimitiveType(List<GroupType> path, PrimitiveType primitiveType) {
        PrimitiveTypeName parquetType = primitiveType.getPrimitiveTypeName();
        Schema.Type kafkaType = Objects.requireNonNull(P2K_PRIMITIVE_TYPE.get(parquetType), "Type mapping is undefined for Parquet: " + parquetType);
        SchemaBuilder kafkaSchema = SchemaBuilder.type(kafkaType).name(primitiveType.getName());
        if (! primitiveType.isRepetition(Type.Repetition.REQUIRED)) {
            kafkaSchema = kafkaSchema.optional();
        }
        return kafkaSchema.build();
    }

    @Override
    public Schema convertGroupType(List<GroupType> path, GroupType groupType, List<Schema> children) {
        return structOf(groupType, children);
    }

    @Override
    public Schema convertMessageType(MessageType messageType, List<Schema> children) {
        return structOf(messageType, children);
    }

    private static Schema structOf(Type type, List<Schema> children) {
        SchemaBuilder struct = SchemaBuilder.struct().name(type.getName());
        if (! type.isRepetition(Type.Repetition.REQUIRED)) {
            struct = struct.optional();
        }
        for (Schema c : children) {
            struct.field(c.name(), c);
        }
        return struct.build();
    }
}
