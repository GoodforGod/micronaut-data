package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.model.query.builder.sql.SqlQueryBuilder;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations.OperationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class ReactiveCascadeOperations<Ctx extends OperationContext> extends AbstractCascadeOperations {

    private static final Logger LOG = LoggerFactory.getLogger(ReactiveCascadeOperations.class);

    private final ReactiveCascadeOperationsHelper<Ctx> helper;

    public ReactiveCascadeOperations(ConversionService<?> conversionService, ReactiveCascadeOperationsHelper<Ctx> helper) {
        super(conversionService);
        this.helper = helper;
    }

    public <T> Mono<T> cascadeEntity(Ctx ctx,
                                     T en, RuntimePersistentEntity<T> persistentEntity,
                                     boolean isPost, Relation.Cascade cascadeType) {
        List<CascadeOp> cascadeOps = new ArrayList<>();

        cascade(ctx.annotationMetadata, ctx.repositoryType, isPost, cascadeType,
                CascadeContext.of(ctx.associations, en, (RuntimePersistentEntity<Object>) persistentEntity), persistentEntity, en, cascadeOps);

        Mono<T> entity = Mono.just(en);

        for (CascadeOp cascadeOp : cascadeOps) {
            if (cascadeOp instanceof CascadeOneOp) {
                CascadeOneOp cascadeOneOp = (CascadeOneOp) cascadeOp;
                Object child = cascadeOneOp.child;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeOneOp.childPersistentEntity;
                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();

                if (ctx.persisted.contains(child)) {
                    continue;
                }
                boolean hasId = childPersistentEntity.getIdentity().getProperty().get(child) != null;

                Mono<Object> childMono;
                if (!hasId && (cascadeType == Relation.Cascade.PERSIST)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                    }
                    Mono<Object> persisted = helper.persistOneSync(ctx, child, childPersistentEntity);
                    entity = entity.flatMap(e -> persisted.map(persistedEntity -> afterCascadedOne(e, cascadeOp.ctx.associations, child, persistedEntity)));
                    childMono = persisted;
                } else if (hasId && (cascadeType == Relation.Cascade.UPDATE)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading MERGE for '{}' ({}) association: '{}'", persistentEntity.getName(),
                                persistentEntity.getIdentity().getProperty().get(en), cascadeOp.ctx.associations);
                    }
                    Mono<Object> updated = helper.updateOneReactive(ctx, child, childPersistentEntity);
                    entity = entity.flatMap(e -> updated.map(updatedEntity -> afterCascadedOne(e, cascadeOp.ctx.associations, child, updatedEntity)));
                    childMono = updated;
                } else {
                    childMono = Mono.just(child);
                }

                if (!hasId
                        && (cascadeType == Relation.Cascade.PERSIST || cascadeType == Relation.Cascade.UPDATE)
                        && SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {
                    entity = entity.flatMap(e -> childMono.flatMap(c -> {
                        if (ctx.persisted.contains(c)) {
                            return Mono.just(e);
                        }
                        ctx.persisted.add(c);
                        Mono<Void> op = helper.persistManyAssociationReactive(ctx, association, e, (RuntimePersistentEntity<Object>) persistentEntity, c, childPersistentEntity);
                        return op.thenReturn(e);
                    }));
                } else {
                    entity = entity.flatMap(e -> childMono.map(c -> {
                        ctx.persisted.add(c);
                        return e;
                    }));
                }
            } else if (cascadeOp instanceof CascadeManyOp) {
                CascadeManyOp cascadeManyOp = (CascadeManyOp) cascadeOp;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeManyOp.childPersistentEntity;

                Mono<List<Object>> children;
                if (cascadeType == Relation.Cascade.UPDATE) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading UPDATE for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                    }
                    Flux<Object> childrenFlux = Flux.empty();
                    for (Object child : cascadeManyOp.children) {
                        if (ctx.persisted.contains(child)) {
                            continue;
                        }
                        Mono<Object> modifiedEntity;
                        if (childPersistentEntity.getIdentity().getProperty().get(child) == null) {
                            modifiedEntity = helper.persistOneSync(ctx, child, childPersistentEntity);
                        } else {
                            modifiedEntity = helper.updateOneReactive(ctx, child, childPersistentEntity);
                        }
                        childrenFlux = childrenFlux.concatWith(modifiedEntity);
                    }
                    children = childrenFlux.collectList();
                } else if (cascadeType == Relation.Cascade.PERSIST) {
                    if (helper.supportsBatch(persistentEntity)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                        }
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        Predicate<Object> veto = val -> ctx.persisted.contains(val) || identity.getProperty().get(val) != null;
                        Flux<Object> inserted = helper.persistBatchReactive(ctx, cascadeManyOp.children, childPersistentEntity, veto);
                        children = inserted.collectList();
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                        }

                        Flux<Object> childrenFlux = Flux.empty();
                        for (Object child : cascadeManyOp.children) {
                            if (ctx.persisted.contains(child) || childPersistentEntity.getIdentity().getProperty().get(child) != null) {
                                childrenFlux = childrenFlux.concatWith(Mono.just(child));
                                continue;
                            }
                            Mono<Object> persisted = helper.persistOneSync(ctx, child, childPersistentEntity);
                            childrenFlux = childrenFlux.concatWith(persisted);
                        }
                        children = childrenFlux.collectList();
                    }
                } else {
                    continue;
                }
                entity = entity.flatMap(e -> children.flatMap(newChildren -> {
                    T entityAfterCascade = afterCascadedMany(e, cascadeOp.ctx.associations, cascadeManyOp.children, newChildren);
                    RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                    if (SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {
                        if (ctx.dialect.allowBatch()) {
                            Predicate<Object> veto = ctx.persisted::contains;
                            Mono<Void> op = helper.persistManyAssociationBatchReactive(ctx, association, cascadeOp.ctx.parent, cascadeOp.ctx.parentPersistentEntity, newChildren, childPersistentEntity, veto);
                            return op.thenReturn(entityAfterCascade);
                        } else {
                            Mono<T> res = Mono.just(entityAfterCascade);
                            for (Object child : newChildren) {
                                if (ctx.persisted.contains(child)) {
                                    continue;
                                }
                                Mono<Void> op = helper.persistManyAssociationReactive(ctx, association, cascadeOp.ctx.parent, cascadeOp.ctx.parentPersistentEntity, child, childPersistentEntity);
                                res = res.flatMap(op::thenReturn);
                            }
                            return res;
                        }
                    }
                    ctx.persisted.addAll(newChildren);
                    return Mono.just(entityAfterCascade);
                }));

            }
        }
        return entity;
    }

    public interface ReactiveCascadeOperationsHelper<Ctx extends OperationContext> {

        boolean supportsBatch(RuntimePersistentEntity<?> persistentEntity);

        <T> Mono<T> persistOneSync(Ctx ctx, T value, RuntimePersistentEntity<T> persistentEntity);

        Flux<Object> persistBatchReactive(Ctx ctx, Iterable<Object> values,
                                          RuntimePersistentEntity<Object> childPersistentEntity,
                                          Predicate<Object> predicate);

        Mono<Object> updateOneReactive(Ctx ctx, Object child, RuntimePersistentEntity<Object> childPersistentEntity);

        Mono<Void> persistManyAssociationReactive(Ctx ctx,
                                                  RuntimeAssociation runtimeAssociation,
                                                  Object value, RuntimePersistentEntity<Object> persistentEntity,
                                                  Object child, RuntimePersistentEntity<Object> childPersistentEntity);

        Mono<Void> persistManyAssociationBatchReactive(Ctx ctx,
                                                       RuntimeAssociation runtimeAssociation,
                                                       Object value, RuntimePersistentEntity<Object> persistentEntity,
                                                       Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity,
                                                       Predicate<Object> veto);
    }

}
