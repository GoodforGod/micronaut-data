package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.model.query.builder.sql.SqlQueryBuilder;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

public class SyncCascadeOperations<Ctx extends OperationContext> extends AbstractCascadeOperations {

    private static final Logger LOG = LoggerFactory.getLogger(SyncCascadeOperations.class);
    private final SyncCascadeOperationsHelper<Ctx> helper;

    public SyncCascadeOperations(ConversionService<?> conversionService, SyncCascadeOperationsHelper<Ctx> helper) {
        super(conversionService);
        this.helper = helper;
    }

    public <T> T cascadeEntity(Ctx ctx,
                               T entity,
                               RuntimePersistentEntity<T> persistentEntity,
                               boolean isPost,
                               Relation.Cascade cascadeType) {
        List<CascadeOp> cascadeOps = new ArrayList<>();
        cascade(ctx.annotationMetadata, ctx.repositoryType,
                isPost, cascadeType,
                CascadeContext.of(ctx.associations, entity, (RuntimePersistentEntity<Object>) persistentEntity),
                persistentEntity, entity, cascadeOps);
        for (CascadeOp cascadeOp : cascadeOps) {
            if (cascadeOp instanceof CascadeOneOp) {
                CascadeOneOp cascadeOneOp = (CascadeOneOp) cascadeOp;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeOp.childPersistentEntity;
                Object child = cascadeOneOp.child;
                if (ctx.persisted.contains(child)) {
                    continue;
                }
                boolean hasId = childPersistentEntity.getIdentity().getProperty().get(child) != null;
                if (!hasId && (cascadeType == Relation.Cascade.PERSIST)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                    }
                    Object persisted = helper.persistOne(ctx, child, childPersistentEntity);
                    entity = afterCascadedOne(entity, cascadeOp.ctx.associations, child, persisted);
                    child = persisted;
                } else if (hasId && (cascadeType == Relation.Cascade.UPDATE)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading MERGE for '{}' ({}) association: '{}'", persistentEntity.getName(),
                                persistentEntity.getIdentity().getProperty().get(entity), cascadeOp.ctx.associations);
                    }
                    Object updated = helper.updateOne(ctx, child, childPersistentEntity);
                    entity = afterCascadedOne(entity, cascadeOp.ctx.associations, child, updated);
                    child = updated;
                }
                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                if (!hasId
                        && (cascadeType == Relation.Cascade.PERSIST || cascadeType == Relation.Cascade.UPDATE)
                        && SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {

                    helper.persistManyAssociation(ctx, association, entity, (RuntimePersistentEntity<Object>) persistentEntity, child, childPersistentEntity);
                }
                ctx.persisted.add(child);
            } else if (cascadeOp instanceof CascadeManyOp) {
                CascadeManyOp cascadeManyOp = (CascadeManyOp) cascadeOp;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeManyOp.childPersistentEntity;

                List<Object> entities;
                if (cascadeType == Relation.Cascade.UPDATE) {
                    entities = CollectionUtils.iterableToList(cascadeManyOp.children);
                    for (ListIterator<Object> iterator = entities.listIterator(); iterator.hasNext(); ) {
                        Object child = iterator.next();
                        if (ctx.persisted.contains(child)) {
                            continue;
                        }
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        Object value;
                        if (identity.getProperty().get(child) == null) {
                            value = helper.persistOne(ctx, child, childPersistentEntity);
                        } else {
                            value = helper.updateOne(ctx, child, childPersistentEntity);
                        }
                        iterator.set(value);
                    }
                } else if (cascadeType == Relation.Cascade.PERSIST) {
                    if (helper.supportsBatch(ctx, childPersistentEntity)) {
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        Predicate<Object> veto = val -> ctx.persisted.contains(val) || identity.getProperty().get(val) != null;
                        entities = helper.persistBatch(ctx, cascadeManyOp.children, childPersistentEntity, veto);
                    } else {
                        entities = CollectionUtils.iterableToList(cascadeManyOp.children);
                        for (ListIterator<Object> iterator = entities.listIterator(); iterator.hasNext(); ) {
                            Object child = iterator.next();
                            if (ctx.persisted.contains(child)) {
                                continue;
                            }
                            RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                            if (identity.getProperty().get(child) != null) {
                                continue;
                            }
                            Object persisted = helper.persistOne(ctx, child, childPersistentEntity);
                            iterator.set(persisted);
                        }
                    }
                } else {
                    continue;
                }

                entity = afterCascadedMany(entity, cascadeOp.ctx.associations, cascadeManyOp.children, entities);

                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                if (SqlQueryBuilder.isForeignKeyWithJoinTable(association) && !entities.isEmpty()) {
                    if (helper.supportsBatch(ctx, childPersistentEntity)) {
                        helper.persistManyAssociationBatch(ctx, association,
                                cascadeOp.ctx.parent, cascadeOp.ctx.parentPersistentEntity, entities, childPersistentEntity);
                    } else {
                        for (Object e : cascadeManyOp.children) {
                            if (ctx.persisted.contains(e)) {
                                continue;
                            }
                            helper.persistManyAssociation(ctx, association,
                                    cascadeOp.ctx.parent, cascadeOp.ctx.parentPersistentEntity, e, childPersistentEntity);
                        }
                    }
                }
                ctx.persisted.addAll(entities);
            }
        }
        return entity;
    }

    public interface SyncCascadeOperationsHelper<Ctx extends OperationContext> {

        boolean supportsBatch(Ctx ctx, RuntimePersistentEntity<?> persistentEntity);

        <T> T persistOne(Ctx ctx, T child, RuntimePersistentEntity<T> childPersistentEntity);

        <T> List<T> persistBatch(Ctx ctx, Iterable<T> values,
                                 RuntimePersistentEntity<T> childPersistentEntity,
                                 Predicate<T> predicate);

        <T> T updateOne(Ctx ctx, T child, RuntimePersistentEntity<T> childPersistentEntity);

        void persistManyAssociation(Ctx ctx,
                                    RuntimeAssociation runtimeAssociation,
                                    Object value, RuntimePersistentEntity<Object> persistentEntity,
                                    Object child, RuntimePersistentEntity<Object> childPersistentEntity);

        void persistManyAssociationBatch(Ctx ctx,
                                         RuntimeAssociation runtimeAssociation,
                                         Object value, RuntimePersistentEntity<Object> persistentEntity,
                                         Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity);
    }


}
