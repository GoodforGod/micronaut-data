package io.micronaut.data.document.mongodb.serde;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.serde.Serializer;
import io.micronaut.serde.exceptions.SerdeException;
import io.micronaut.serde.reference.PropertyReference;
import io.micronaut.serde.reference.SerializationReference;

public class DataEncoderContext implements Serializer.EncoderContext {

    private final RuntimePersistentEntity<Object> runtimePersistentEntity;
    private final Serializer.EncoderContext parent;

    public DataEncoderContext(RuntimePersistentEntity<Object> runtimePersistentEntity, Serializer.EncoderContext parent) {
        this.runtimePersistentEntity = runtimePersistentEntity;
        this.parent = parent;
    }

    @Override
    public ConversionService<?> getConversionService() {
        return parent.getConversionService();
    }

    @Override
    public boolean hasView(Class<?>... views) {
        return parent.hasView(views);
    }

    @Override
    public <B, P> SerializationReference<B, P> resolveReference(SerializationReference<B, P> reference) {
        return parent.resolveReference(reference);
    }

    @Override
    public <T, D extends Serializer<? extends T>> D findCustomSerializer(Class<? extends D> serializerClass) throws SerdeException {
        return parent.findCustomSerializer(serializerClass);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Argument<? extends T> forType) throws SerdeException {
        return parent.findSerializer(forType);
    }

    @Override
    public <T> Serializer<? super T> findSerializer(Class<? extends T> forType) throws SerdeException {
        return parent.findSerializer(forType);
    }

    @Override
    public <B, P> void pushManagedRef(PropertyReference<B, P> reference) {
        parent.pushManagedRef(reference);
    }

    @Override
    public void popManagedRef() {
        parent.popManagedRef();
    }
}
