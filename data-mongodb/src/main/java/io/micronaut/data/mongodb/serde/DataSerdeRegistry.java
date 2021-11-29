package io.micronaut.data.mongodb.serde;

import io.micronaut.core.beans.BeanIntrospection;
import io.micronaut.core.beans.BeanIntrospector;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.SerdeRegistry;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.util.TypeKey;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DataSerdeRegistry implements SerdeRegistry {

    private final SerdeRegistry serdeRegistry;
    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final Map<TypeKey, Serializer<?>> serializerMap = new ConcurrentHashMap<>(50);
    private final Map<TypeKey, Serializer<?>> deserializerMap = new ConcurrentHashMap<>(50);

    public DataSerdeRegistry(SerdeRegistry serdeRegistry, RuntimeEntityRegistry runtimeEntityRegistry) {
        this.serdeRegistry = serdeRegistry;
        this.runtimeEntityRegistry = runtimeEntityRegistry;
    }

    @Override
    public Serializer.EncoderContext newEncoderContext(Class<?> view) {
        return serdeRegistry.newEncoderContext(view);
    }

    @Override
    public Deserializer.DecoderContext newDecoderContext(Class<?> view) {
        return serdeRegistry.newDecoderContext(view);
    }

    @Override
    public <T, D extends Deserializer<? extends T>> D findCustomDeserializer(Class<? extends D> deserializerClass) throws SerdeException {
        return serdeRegistry.findCustomDeserializer(deserializerClass);
    }

    @Override
    public <T> Deserializer<? extends T> findDeserializer(Argument<? extends T> type) throws SerdeException {
        return serdeRegistry.findDeserializer(type);
    }

    @Override
    public <T> Deserializer<? extends T> findDeserializer(Class<? extends T> type) throws SerdeException {
        return serdeRegistry.findDeserializer(type);
    }

    @Override
    public <T> Collection<BeanIntrospection<? extends T>> getDeserializableSubtypes(Class<T> superType) {
        return serdeRegistry.getDeserializableSubtypes(superType);
    }

    @Override
    public <T, D extends Serializer<? extends T>> D findCustomSerializer(Class<? extends D> serializerClass) throws SerdeException {
        return serdeRegistry.findCustomSerializer(serializerClass);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Argument<? extends T> forType) throws SerdeException {
        Class<? extends T> type = forType.getType();
        Optional<? extends BeanIntrospection<? extends T>> introspection = BeanIntrospector.SHARED.findIntrospection(type);
        if (introspection.isPresent()) {
            if (introspection.get().isAnnotationPresent(MappedEntity.class)) {

            }
        }
        return serdeRegistry.findSerializer(forType);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Class<? extends T> forType) throws SerdeException {
        return serdeRegistry.findSerializer(forType);
    }

}
