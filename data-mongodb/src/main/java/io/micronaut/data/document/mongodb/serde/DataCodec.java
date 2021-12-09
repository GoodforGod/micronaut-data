package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.type.Argument;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.bson.BsonReaderDecoder;
import io.micronaut.serde.bson.BsonWriterEncoder;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.io.IOException;

public class DataCodec<T> implements Codec<T> {

    private final DataSerdeRegistry dataSerdeRegistry;
    private final RuntimePersistentEntity<T> persistentEntity;
    private final Class<T> type;
    private final Argument<T> argument;
    private final Serializer<? super T> serializer;
    private final Deserializer<? extends T> deserializer;

    public DataCodec(DataSerdeRegistry dataSerdeRegistry, RuntimePersistentEntity<T> persistentEntity, Class<T> type) {
        this.dataSerdeRegistry = dataSerdeRegistry;
        this.persistentEntity = persistentEntity;
        this.type = type;
        this.argument = Argument.of(type);
        try {
            this.serializer = dataSerdeRegistry.findSerializer(argument);
            this.deserializer = dataSerdeRegistry.findDeserializer(argument);
        } catch (IOException e) {
            throw new DataAccessException("Cannot find serialize/deserializer for type: " + type + ". " + e.getMessage(), e);
        }
    }

    @Override
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        try {
            T deserialize = deserializer.deserialize(new BsonReaderDecoder(reader), dataSerdeRegistry.newDecoderContext(type), argument);
            return deserialize;
        } catch (IOException e) {
            throw new DataAccessException("Cannot deserialize: " + type, e);
        }
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
        try {
            serializer.serialize(new BsonWriterEncoder(writer, false), dataSerdeRegistry.newEncoderContext(type, (RuntimePersistentEntity<Object>) persistentEntity), value, argument);
        } catch (IOException e) {
            throw new DataAccessException("Cannot serialize: " + value, e);
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        return type;
    }
}
