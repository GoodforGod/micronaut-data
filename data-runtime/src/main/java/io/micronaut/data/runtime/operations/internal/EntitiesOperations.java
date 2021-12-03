package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;

/**
 * The entities operations container.
 *
 * @param <T> The entity type
 */
abstract class EntitiesOperations<T, Ext extends Exception> extends BaseOperations<T, Ext> {

    EntitiesOperations(EntityEventListener<Object> entityEventListener, RuntimePersistentEntity<T> persistentEntity, ConversionService<?> conversionService) {
        super(entityEventListener, persistentEntity, conversionService);
    }

}
