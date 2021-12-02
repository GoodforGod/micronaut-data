package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import reactor.core.publisher.Mono;

/**
 * The entity operations container.
 *
 * @param <T> The entity type
 */
public abstract class ReactiveEntityOperations<T, Exc extends Exception> extends EntityOperations<T, Exc> {

    public ReactiveEntityOperations(EntityEventListener<Object> entityEventListener, RuntimePersistentEntity<T> persistentEntity, ConversionService<?> conversionService) {
        super(entityEventListener, persistentEntity, conversionService);
    }

    public abstract Mono<T> getEntity();

}
