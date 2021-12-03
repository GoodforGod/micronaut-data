package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractEntitiesOperations<Ctx extends AbstractRepositoryOperations.OperationContext, T, Exc extends Exception> extends SyncEntitiesOperations<T, Exc> {

    protected final Ctx ctx;
    protected final SyncCascadeOperations<Ctx> cascadeOperations;
    protected final ConversionService<?> conversionService;
    protected final List<Data> entities;
    protected final boolean insert;
    protected final boolean hasGeneratedId;

    protected AbstractEntitiesOperations(Ctx ctx,
                                       SyncCascadeOperations<Ctx> cascadeOperations,
                                       ConversionService<?> conversionService,
                                       EntityEventListener<Object> entityEventListener,
                                       RuntimePersistentEntity<T> persistentEntity,
                                       Iterable<T> entities,
                                       boolean insert) {
        super(entityEventListener, persistentEntity, conversionService);
        this.cascadeOperations = cascadeOperations;
        this.conversionService = conversionService;
        this.ctx = ctx;
        this.insert = insert;
        this.hasGeneratedId = insert && persistentEntity.getIdentity() != null && persistentEntity.getIdentity().isGenerated();
        Objects.requireNonNull(entities, "Entities cannot be null");
        if (!entities.iterator().hasNext()) {
            throw new IllegalStateException("Entities cannot be empty");
        }
        Stream<T> stream;
        if (entities instanceof Collection) {
            stream = ((Collection) entities).stream();
        } else {
            stream = CollectionUtils.iterableToList(entities).stream();
        }
        this.entities = stream.map(entity -> {
            Data d = new Data();
            d.entity = entity;
            return d;
        }).collect(Collectors.toList());
    }

    @Override
    protected void cascadePre(Relation.Cascade cascadeType) {
        for (Data d : entities) {
            if (d.vetoed) {
                continue;
            }
            d.entity = cascadeOperations.cascadeEntity(ctx, d.entity, persistentEntity, false, cascadeType);
        }
    }

    @Override
    protected void cascadePost(Relation.Cascade cascadeType) {
        for (Data d : entities) {
            if (d.vetoed) {
                continue;
            }
            d.entity = cascadeOperations.cascadeEntity(ctx, d.entity, persistentEntity, true, cascadeType);
        }
    }

    @Override
    protected void collectAutoPopulatedPreviousValues() {
//        for (Data d : entities) {
//            if (d.vetoed) {
//                continue;
//            }
//            d.previousValues = dbOperation.collectAutoPopulatedPreviousValues(persistentEntity, d.entity);
//        }
    }

    @Override
    public void veto(Predicate<T> predicate) {
        for (Data d : entities) {
            if (d.vetoed) {
                continue;
            }
            d.vetoed = predicate.test(d.entity);
        }
    }

    @Override
    protected boolean triggerPre(Function<EntityEventContext<Object>, Boolean> fn) {
        boolean allVetoed = true;
        for (Data d : entities) {
            if (d.vetoed) {
                continue;
            }
            final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, d.entity);
            if (!fn.apply((EntityEventContext<Object>) event)) {
                d.vetoed = true;
                continue;
            }
            d.entity = event.getEntity();
            allVetoed = false;
        }
        return allVetoed;
    }

    @Override
    protected void triggerPost(Consumer<EntityEventContext<Object>> fn) {
        for (Data d : entities) {
            if (d.vetoed) {
                continue;
            }
            final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, d.entity);
            fn.accept((EntityEventContext<Object>) event);
            d.entity = event.getEntity();
        }
    }

    public List<T> getEntities() {
        return entities.stream().map(d -> d.entity).collect(Collectors.toList());
    }

    protected class Data {
        public T entity;
        public Map<QueryParameterBinding, Object> previousValues;
        public boolean vetoed = false;
    }
}