package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class AbstractReactiveEntitiesOperations<Ctx extends OperationContext, T, Exc extends Exception> extends ReactiveEntitiesOperations<T, Exc> {

    protected final Ctx ctx;
    protected final ReactiveCascadeOperations<Ctx> cascadeOperations;
    protected final boolean insert;
    protected final boolean hasGeneratedId;
    protected Flux<Data> entities;
    protected Mono<Integer> rowsUpdated;

    protected AbstractReactiveEntitiesOperations(Ctx ctx,
                                                 ReactiveCascadeOperations<Ctx> cascadeOperations,
                                                 ConversionService<?> conversionService,
                                                 EntityEventListener<Object> entityEventListener,
                                                 RuntimePersistentEntity<T> persistentEntity,
                                                 Iterable<T> entities,
                                                 boolean insert) {
        super(entityEventListener, persistentEntity, conversionService);
        this.ctx = ctx;
        this.cascadeOperations = cascadeOperations;
        this.insert = insert;
        this.hasGeneratedId = insert && persistentEntity.getIdentity() != null && persistentEntity.getIdentity().isGenerated();
        Objects.requireNonNull(entities, "Entities cannot be null");
        if (!entities.iterator().hasNext()) {
            throw new IllegalStateException("Entities cannot be empty");
        }
        this.entities = Flux.fromIterable(entities).map(entity -> {
            Data data = new Data();
            data.entity = entity;
            return data;
        });
    }

    @Override
    protected void cascadePre(Relation.Cascade cascadeType) {
        doCascade(false, cascadeType);
    }

    @Override
    protected void cascadePost(Relation.Cascade cascadeType) {
        doCascade(true, cascadeType);
    }

    private void doCascade(boolean isPost, Relation.Cascade cascadeType) {
        this.entities = entities.flatMap(d -> {
            if (d.vetoed) {
                return Mono.just(d);
            }
            Mono<T> entity = cascadeOperations.cascadeEntity(ctx, d.entity, persistentEntity, isPost, cascadeType);
            return entity.map(e -> {
                d.entity = e;
                return d;
            });
        });
    }

    @Override
    public void veto(Predicate<T> predicate) {
        entities = entities.map(d -> {
            if (d.vetoed) {
                return d;
            }
            d.vetoed = predicate.test(d.entity);
            return d;
        });
    }

    @Override
    protected boolean triggerPre(Function<EntityEventContext<Object>, Boolean> fn) {
        entities = entities.map(d -> {
            if (d.vetoed) {
                return d;
            }
            final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, d.entity);
            d.vetoed = !fn.apply((EntityEventContext<Object>) event);
            d.entity = event.getEntity();
            return d;
        });
        return false;
    }

    @Override
    protected void triggerPost(Consumer<EntityEventContext<Object>> fn) {
        entities = entities.map(d -> {
            if (d.vetoed) {
                return d;
            }
            final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, d.entity);
            fn.accept((EntityEventContext<Object>) event);
            d.entity = event.getEntity();
            return d;
        });
    }

    protected boolean notVetoed(Data data) {
        return !data.vetoed;
    }

    public Flux<T> getEntities() {
        return entities.map(d -> d.entity);
    }

    public Mono<Integer> getRowsUpdated() {
        // We need to trigger entities to execute post actions when getting just rows
        return rowsUpdated.flatMap(rows -> entities.then(Mono.just(rows)));
    }

    protected final class Data {
        public T entity;
        public Map<QueryParameterBinding, Object> previousValues;
        public boolean vetoed = false;
    }
}