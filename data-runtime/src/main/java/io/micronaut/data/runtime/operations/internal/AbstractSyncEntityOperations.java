package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class AbstractSyncEntityOperations<Ctx extends OperationContext, T, Exc extends Exception> extends SyncEntityOperations<T, Exc> {

    protected SyncCascadeOperations<Ctx> cascadeOperations;
    protected final Ctx ctx;
    protected final boolean insert;
    protected final boolean hasGeneratedId;
    protected T entity;
    protected Map<QueryParameterBinding, Object> previousValues;

    protected AbstractSyncEntityOperations(Ctx ctx,
                                           SyncCascadeOperations<Ctx> cascadeOperations,
                                           EntityEventListener<Object> entityEventListener,
                                           RuntimePersistentEntity<T> persistentEntity,
                                           ConversionService<?> conversionService,
                                           T entity,
                                           boolean insert) {
        super(entityEventListener, persistentEntity, conversionService);
        this.cascadeOperations = cascadeOperations;
        this.ctx = ctx;
        this.insert = insert;
        this.hasGeneratedId = insert && persistentEntity.getIdentity() != null && persistentEntity.getIdentity().isGenerated();
        Objects.requireNonNull(entity, "Passed entity cannot be null");
        this.entity = entity;
    }

    @Override
    protected void cascadePre(Relation.Cascade cascadeType) {
        entity = cascadeOperations.cascadeEntity(ctx, entity, persistentEntity, false, cascadeType);
    }

    @Override
    protected void cascadePost(Relation.Cascade cascadeType) {
        entity = cascadeOperations.cascadeEntity(ctx, entity, persistentEntity, true, cascadeType);
    }

    @Override
    protected void collectAutoPopulatedPreviousValues() {
//        previousValues = dbOperation.collectAutoPopulatedPreviousValues(persistentEntity, entity);
    }

    @Override
    protected boolean triggerPre(Function<EntityEventContext<Object>, Boolean> fn) {
        final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, entity);
        boolean vetoed = !fn.apply((EntityEventContext<Object>) event);
        if (vetoed) {
            return true;
        }
        T newEntity = event.getEntity();
        if (entity != newEntity) {
            entity = newEntity;
        }
        return false;
    }

    @Override
    protected void triggerPost(Consumer<EntityEventContext<Object>> fn) {
        final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, entity);
        fn.accept((EntityEventContext<Object>) event);
    }

    @Override
    public void veto(Predicate<T> predicate) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public T getEntity() {
        return entity;
    }
}
