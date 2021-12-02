package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import reactor.core.publisher.Flux;

/**
 * The entities operations container.
 *
 * @param <T> The entity type
 */
public abstract class ReactiveEntitiesOperations<T, Exc extends Exception> extends EntitiesOperations<T, Exc> {

    public ReactiveEntitiesOperations(EntityEventListener<Object> entityEventListener, RuntimePersistentEntity<T> persistentEntity, ConversionService<?> conversionService) {
        super(entityEventListener, persistentEntity, conversionService);
    }

    public abstract Flux<T> getEntities();
}
