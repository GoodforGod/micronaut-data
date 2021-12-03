package io.micronaut.data.mongodb.serde;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.beans.BeanIntrospection;
import io.micronaut.core.beans.BeanIntrospector;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.DefaultSerdeRegistry;
import io.micronaut.serde.Deserializer;
import io.micronaut.serde.Serde;
import io.micronaut.serde.SerdeIntrospections;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.deserializers.ObjectDeserializer;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.serializers.ObjectSerializer;
import io.micronaut.serde.util.TypeKey;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@Replaces(DefaultSerdeRegistry.class)
public class DataSerdeRegistry extends DefaultSerdeRegistry {

    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final Map<TypeKey, Serializer<?>> serializerMap = new ConcurrentHashMap<>(50);
    private final Map<TypeKey, Serializer<?>> deserializerMap = new ConcurrentHashMap<>(50);

    public DataSerdeRegistry(BeanContext beanContext,
                             ObjectSerializer objectSerializer,
                             ObjectDeserializer objectDeserializer,
                             Serde<Object[]> objectArraySerde,
                             SerdeIntrospections introspections,
                             RuntimeEntityRegistry runtimeEntityRegistry) {
        super(beanContext, objectSerializer, objectDeserializer, objectArraySerde, introspections);
        this.runtimeEntityRegistry = runtimeEntityRegistry;
    }

    public Serializer.EncoderContext newEncoderContext(Class<?> view, RuntimePersistentEntity<Object> runtimePersistentEntity) {
        return new DataEncoderContext(runtimePersistentEntity, super.newEncoderContext(view));
    }

    @Override
    public Serializer.EncoderContext newEncoderContext(Class<?> view) {
        return super.newEncoderContext(view);
    }

    @Override
    public Deserializer.DecoderContext newDecoderContext(Class<?> view) {
        return super.newDecoderContext(view);
    }

    @Override
    public <T, D extends Deserializer<? extends T>> D findCustomDeserializer(Class<? extends D> deserializerClass) throws SerdeException {
        return super.findCustomDeserializer(deserializerClass);
    }

    @Override
    public <T> Deserializer<? extends T> findDeserializer(Argument<? extends T> type) {
        return super.findDeserializer(type);
    }

    @Override
    public <T> Deserializer<? extends T> findDeserializer(Class<? extends T> type) throws SerdeException {
        return super.findDeserializer(type);
    }

    @Override
    public <T> Collection<BeanIntrospection<? extends T>> getDeserializableSubtypes(Class<T> superType) {
        return super.getDeserializableSubtypes(superType);
    }

    @Override
    public <T, D extends Serializer<? extends T>> D findCustomSerializer(Class<? extends D> serializerClass) throws SerdeException {
        return super.findCustomSerializer(serializerClass);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Argument<? extends T> forType) throws SerdeException {
        Class<? extends T> type = forType.getType();
        Optional<? extends BeanIntrospection<? extends T>> introspection = BeanIntrospector.SHARED.findIntrospection(type);
        if (introspection.isPresent()) {
            if (introspection.get().isAnnotationPresent(MappedEntity.class)) {

            }
        }
        return super.findSerializer(forType);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Class<? extends T> forType) throws SerdeException {
        return super.findSerializer(forType);
    }

}
