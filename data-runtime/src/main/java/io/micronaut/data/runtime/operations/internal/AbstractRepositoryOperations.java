/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.data.runtime.operations.internal;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextProvider;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.exceptions.ConversionErrorException;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.exceptions.OptimisticLockException;
import io.micronaut.data.model.Association;
import io.micronaut.data.model.Embedded;
import io.micronaut.data.model.PersistentEntity;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.query.JoinPath;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.query.builder.sql.SqlQueryBuilder;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.data.runtime.config.DataSettings;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.MediaTypeCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Abstract SQL repository implementation not specifically bound to JDBC.
 *
 * @param <Cnt> The connection type
 * @param <Exc> The exception type
 * @author Denis Stepanov
 * @since 3.1.0
 */
@SuppressWarnings("FileLength")
@Internal
public abstract class AbstractRepositoryOperations<Cnt, PS, Exc extends Exception>
        implements ApplicationContextProvider, OpContext<Cnt, PS> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractRepositoryOperations.class);
    protected static final Logger QUERY_LOG = DataSettings.QUERY_LOG;
    protected final MediaTypeCodec jsonCodec;
    protected final EntityEventListener<Object> entityEventRegistry;
    protected final DateTimeProvider dateTimeProvider;
    protected final RuntimeEntityRegistry runtimeEntityRegistry;
    protected final DataConversionService<?> conversionService;
    protected final AttributeConverterRegistry attributeConverterRegistry;
    private final Map<Class, RuntimePersistentProperty> idReaders = new ConcurrentHashMap<>(10);

    /**
     * Default constructor.
     *
     * @param codecs                     The media type codecs
     * @param dateTimeProvider           The date time provider
     * @param runtimeEntityRegistry      The entity registry
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     */
    protected AbstractRepositoryOperations(
            List<MediaTypeCodec> codecs,
            DateTimeProvider<Object> dateTimeProvider,
            RuntimeEntityRegistry runtimeEntityRegistry,
            DataConversionService<?> conversionService,
            AttributeConverterRegistry attributeConverterRegistry) {
        this.dateTimeProvider = dateTimeProvider;
        this.runtimeEntityRegistry = runtimeEntityRegistry;
        this.entityEventRegistry = runtimeEntityRegistry.getEntityEventListener();
        this.jsonCodec = resolveJsonCodec(codecs);
        this.conversionService = conversionService;
        this.attributeConverterRegistry = attributeConverterRegistry;
    }

    @Override
    public ApplicationContext getApplicationContext() {
        return runtimeEntityRegistry.getApplicationContext();
    }

    private MediaTypeCodec resolveJsonCodec(List<MediaTypeCodec> codecs) {
        return CollectionUtils.isNotEmpty(codecs) ? codecs.stream().filter(c -> c.getMediaTypes().contains(MediaType.APPLICATION_JSON_TYPE)).findFirst().orElse(null) : null;
    }

    @NonNull
    public final <T> RuntimePersistentEntity<T> getEntity(@NonNull Class<T> type) {
        return runtimeEntityRegistry.getEntity(type);
    }

    @Override
    public RuntimeEntityRegistry getRuntimeEntityRegistry() {
        return runtimeEntityRegistry;
    }

    protected <T> SyncEntityOperations<T> persistOneOp(Cnt cnt, RuntimePersistentEntity<T> persistentEntity, T value, OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected <T> SyncEntitiesOperations<T> persistBatchOp(Cnt cnt, RuntimePersistentEntity<T> persistentEntity, Iterable<T> values, OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected <T> SyncEntityOperations<T> updateOneOp(Cnt cnt, RuntimePersistentEntity<T> persistentEntity, T value, OperationContext operationContext) {
        throw new IllegalStateException();
    }

    private Object persistOneSync(Cnt cnt, Object child, RuntimePersistentEntity<Object> childPersistentEntity, OperationContext operationContext) {
        SyncEntityOperations<Object> persistOneOp = persistOneOp(cnt, childPersistentEntity, child, operationContext);
        persistOneSync(cnt, persistOneOp, operationContext);
        return persistOneOp.getEntity();
    }

    private List<Object> persistBatchSync(Cnt cnt, Iterable<Object> values,
                                          RuntimePersistentEntity<Object> childPersistentEntity,
                                          Predicate<Object> predicate,
                                          OperationContext operationContext) {
        SyncEntitiesOperations<Object> persistBatchOp = persistBatchOp(cnt, childPersistentEntity, values, operationContext);
        persistBatchOp.veto(predicate);
        persistInBatch(cnt, persistBatchOp, operationContext);
        return persistBatchOp.getEntities();
    }

    private Object updateOneSync(Cnt cnt, Object child, RuntimePersistentEntity<Object> childPersistentEntity, OperationContext operationContext) {
        SyncEntityOperations<Object> updateOneOp = updateOneOp(cnt, childPersistentEntity, child, operationContext);
        updateOneSync(cnt, updateOneOp, operationContext);
        return updateOneOp.getEntity();
    }

    protected void persistManyAssociationSync(Cnt cnt,
                                              RuntimeAssociation runtimeAssociation,
                                              Object value, RuntimePersistentEntity<Object> persistentEntity,
                                              Object child, RuntimePersistentEntity<Object> childPersistentEntity,
                                              OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected void persistManyAssociationBatchSync(Cnt cnt,
                                                   RuntimeAssociation runtimeAssociation,
                                                   Object value, RuntimePersistentEntity<Object> persistentEntity,
                                                   Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity,
                                                   OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected <T> T cascadeEntity(Cnt cnt,
                                  T entity,
                                  RuntimePersistentEntity<T> persistentEntity,
                                  boolean isPost,
                                  Relation.Cascade cascadeType,
                                  OperationContext operationContext) {
        List<CascadeOp> cascadeOps = new ArrayList<>();
        cascade(operationContext.annotationMetadata, operationContext.repositoryType,
                isPost, cascadeType,
                AbstractSqlRepositoryOperations.CascadeContext.of(operationContext.associations, entity),
                persistentEntity, entity, cascadeOps);
        for (CascadeOp cascadeOp : cascadeOps) {
            if (cascadeOp instanceof CascadeOneOp) {
                CascadeOneOp cascadeOneOp = (CascadeOneOp) cascadeOp;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeOp.childPersistentEntity;
                Object child = cascadeOneOp.child;
                if (operationContext.persisted.contains(child)) {
                    continue;
                }
                boolean hasId = childPersistentEntity.getIdentity().getProperty().get(child) != null;
                if (!hasId && (cascadeType == Relation.Cascade.PERSIST)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                    }
                    Object persisted = persistOneSync(cnt, child, childPersistentEntity, operationContext);
                    entity = afterCascadedOne(entity, cascadeOp.ctx.associations, child, persisted);
                    child = persisted;
                } else if (hasId && (cascadeType == Relation.Cascade.UPDATE)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading MERGE for '{}' ({}) association: '{}'", persistentEntity.getName(),
                                persistentEntity.getIdentity().getProperty().get(entity), cascadeOp.ctx.associations);
                    }
                    Object updated = updateOneSync(cnt, child, childPersistentEntity, operationContext);
                    entity = afterCascadedOne(entity, cascadeOp.ctx.associations, child, updated);
                    child = updated;
                }
                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                if (!hasId
                        && (cascadeType == Relation.Cascade.PERSIST || cascadeType == Relation.Cascade.UPDATE)
                        && SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {

                    RuntimePersistentEntity<Object> runtimePersistent = getEntity((Class<Object>) entity.getClass());
                    persistManyAssociationSync(cnt, association, entity, runtimePersistent, child, childPersistentEntity, operationContext);
                }
                operationContext.persisted.add(child);
            } else if (cascadeOp instanceof CascadeManyOp) {
                CascadeManyOp cascadeManyOp = (CascadeManyOp) cascadeOp;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeManyOp.childPersistentEntity;

                List<Object> entities;
                if (cascadeType == Relation.Cascade.UPDATE) {
                    entities = CollectionUtils.iterableToList(cascadeManyOp.children);
                    for (ListIterator<Object> iterator = entities.listIterator(); iterator.hasNext(); ) {
                        Object child = iterator.next();
                        if (operationContext.persisted.contains(child)) {
                            continue;
                        }
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        Object value;
                        if (identity.getProperty().get(child) == null) {
                            value = persistOneSync(cnt, child, childPersistentEntity, operationContext);
                        } else {
                            value = updateOneSync(cnt, child, childPersistentEntity, operationContext);
                        }
                        iterator.set(value);
                    }
                } else if (cascadeType == Relation.Cascade.PERSIST) {
                    if (isSupportsBatchInsert(childPersistentEntity, operationContext.dialect)) {
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        Predicate<Object> veto = val -> operationContext.persisted.contains(val) || identity.getProperty().get(val) != null;
                        entities = persistBatchSync(cnt, cascadeManyOp.children, childPersistentEntity, veto, operationContext);
                    } else {
                        entities = CollectionUtils.iterableToList(cascadeManyOp.children);
                        for (ListIterator<Object> iterator = entities.listIterator(); iterator.hasNext(); ) {
                            Object child = iterator.next();
                            if (operationContext.persisted.contains(child)) {
                                continue;
                            }
                            RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                            if (identity.getProperty().get(child) != null) {
                                continue;
                            }
                            Object persisted = persistOneSync(cnt, child, childPersistentEntity, operationContext);
                            iterator.set(persisted);
                        }
                    }
                } else {
                    continue;
                }

                entity = afterCascadedMany(entity, cascadeOp.ctx.associations, cascadeManyOp.children, entities);

                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                if (SqlQueryBuilder.isForeignKeyWithJoinTable(association) && !entities.isEmpty()) {
                    if (operationContext.dialect.allowBatch()) {
                        Object parent = cascadeOp.ctx.parent;
                        RuntimePersistentEntity<Object> runtimePersistent = getEntity((Class<Object>) parent.getClass());
                        persistManyAssociationBatchSync(cnt, association, parent, runtimePersistent, entities, childPersistentEntity, operationContext);
                    } else {
                        for (Object e : cascadeManyOp.children) {
                            if (operationContext.persisted.contains(e)) {
                                continue;
                            }
                            Object parent = cascadeOp.ctx.parent;
                            RuntimePersistentEntity<Object> runtimePersistent = getEntity((Class<Object>) parent.getClass());
                            persistManyAssociationSync(cnt, association, parent, runtimePersistent, e, childPersistentEntity, operationContext);
                        }
                    }
                }
                operationContext.persisted.addAll(entities);
            }
        }
        return entity;
    }

    protected <T> ReactiveEntityOperations<T> persistOneReactiveOp(Cnt cnt, RuntimePersistentEntity<T> persistentEntity, T value, OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected <T> ReactiveEntitiesOperations<T> persistBatchReactiveOp(Cnt cnt, RuntimePersistentEntity<T> persistentEntity, Iterable<T> values, OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected <T> ReactiveEntityOperations<T> updateOneReactiveOp(Cnt cnt, RuntimePersistentEntity<T> persistentEntity, T value, OperationContext operationContext) {
        throw new IllegalStateException();
    }

    private Mono<Object> persistOneReactive(Cnt cnt, Object child, RuntimePersistentEntity<Object> childPersistentEntity, OperationContext operationContext) {
        ReactiveEntityOperations<Object> persistOneOp = persistOneReactiveOp(cnt, childPersistentEntity, child, operationContext);
        persistOneSync(cnt, persistOneOp, operationContext);
        return persistOneOp.getEntity();
    }

    private Flux<Object> persistBatchReactive(Cnt cnt, Iterable<Object> values,
                                          RuntimePersistentEntity<Object> childPersistentEntity,
                                          Predicate<Object> predicate,
                                          OperationContext operationContext) {
        ReactiveEntitiesOperations<Object> persistBatchOp = persistBatchReactiveOp(cnt, childPersistentEntity, values, operationContext);
        persistBatchOp.veto(predicate);
        persistInBatch(cnt, persistBatchOp, operationContext);
        return persistBatchOp.getEntities();
    }

    private Mono<Object> updateOneReactive(Cnt cnt, Object child, RuntimePersistentEntity<Object> childPersistentEntity, OperationContext operationContext) {
        ReactiveEntityOperations<Object> updateOneOp = updateOneReactiveOp(cnt, childPersistentEntity, child, operationContext);
        updateOneSync(cnt, updateOneOp, operationContext);
        return updateOneOp.getEntity();
    }

    protected Mono<Void> persistManyAssociationReactive(Cnt cnt,
                                              RuntimeAssociation runtimeAssociation,
                                              Object value, RuntimePersistentEntity<Object> persistentEntity,
                                              Object child, RuntimePersistentEntity<Object> childPersistentEntity,
                                              OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected Mono<Void> persistManyAssociationBatchReactive(Cnt cnt,
                                                   RuntimeAssociation runtimeAssociation,
                                                   Object value, RuntimePersistentEntity<Object> persistentEntity,
                                                   Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity,
                                                   Predicate<Object> veto,
                                                   OperationContext operationContext) {
        throw new IllegalStateException();
    }

    protected  <T> Mono<T> cascadeEntityReactive(Cnt cnt,
                                      T en, RuntimePersistentEntity<T> persistentEntity,
                                      boolean isPost, Relation.Cascade cascadeType,
                                      OperationContext operationContext) {
        List<CascadeOp> cascadeOps = new ArrayList<>();

        cascade(operationContext.annotationMetadata, operationContext.repositoryType, isPost, cascadeType, CascadeContext.of(operationContext.associations, en), persistentEntity, en, cascadeOps);

        Mono<T> entity = Mono.just(en);

        for (CascadeOp cascadeOp : cascadeOps) {
            if (cascadeOp instanceof CascadeOneOp) {
                CascadeOneOp cascadeOneOp = (CascadeOneOp) cascadeOp;
                Object child = cascadeOneOp.child;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeOneOp.childPersistentEntity;
                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();

                if (operationContext.persisted.contains(child)) {
                    continue;
                }
                boolean hasId = childPersistentEntity.getIdentity().getProperty().get(child) != null;

                Mono<Object> childMono;
                if (!hasId && (cascadeType == Relation.Cascade.PERSIST)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                    }
                    Mono<Object> persisted = persistOneReactive(cnt, child, childPersistentEntity, operationContext);
                    entity = entity.flatMap(e -> persisted.map(persistedEntity -> afterCascadedOne(e, cascadeOp.ctx.associations, child, persistedEntity)));
                    childMono = persisted;
                } else if (hasId && (cascadeType == Relation.Cascade.UPDATE)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading MERGE for '{}' ({}) association: '{}'", persistentEntity.getName(),
                                persistentEntity.getIdentity().getProperty().get(en), cascadeOp.ctx.associations);
                    }
                    Mono<Object> updated = updateOneReactive(cnt, child, childPersistentEntity, operationContext);
                    entity = entity.flatMap(e -> updated.map(updatedEntity -> afterCascadedOne(e, cascadeOp.ctx.associations, child, updatedEntity)));
                    childMono = updated;
                } else {
                    childMono = Mono.just(child);
                }

                if (!hasId
                        && (cascadeType == Relation.Cascade.PERSIST || cascadeType == Relation.Cascade.UPDATE)
                        && SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {
                    entity = entity.flatMap(e -> childMono.flatMap(c -> {
                        if (operationContext.persisted.contains(c)) {
                            return Mono.just(e);
                        }
                        operationContext.persisted.add(c);
                        RuntimePersistentEntity<Object> entity1 = getEntity((Class<Object>) e.getClass());
                        Mono<Void> op = persistManyAssociationReactive(cnt, association, e, entity1, c, childPersistentEntity, operationContext);
                        return op.thenReturn(e);
                    }));
                } else {
                    entity = entity.flatMap(e -> childMono.map(c -> {
                        operationContext.persisted.add(c);
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
                        if (operationContext.persisted.contains(child)) {
                            continue;
                        }
                        Mono<Object> modifiedEntity;
                        if (childPersistentEntity.getIdentity().getProperty().get(child) == null) {
                            modifiedEntity = persistOneReactive(cnt, child, childPersistentEntity, operationContext);
                        } else {
                            modifiedEntity = updateOneReactive(cnt, child, childPersistentEntity, operationContext);
                        }
                        childrenFlux = childrenFlux.concatWith(modifiedEntity);
                    }
                    children = childrenFlux.collectList();
                } else if (cascadeType == Relation.Cascade.PERSIST) {
                    if (isSupportsBatchInsert(persistentEntity, operationContext.dialect)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                        }
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        Predicate<Object> veto = val -> operationContext.persisted.contains(val) || identity.getProperty().get(val) != null;
                        Flux<Object> inserted = persistBatchReactive(cnt,  cascadeManyOp.children, childPersistentEntity, veto, operationContext);
                        children = inserted.collectList();
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                        }

                        Flux<Object> childrenFlux = Flux.empty();
                        for (Object child : cascadeManyOp.children) {
                            if (operationContext.persisted.contains(child) || childPersistentEntity.getIdentity().getProperty().get(child) != null) {
                                childrenFlux = childrenFlux.concatWith(Mono.just(child));
                                continue;
                            }
                            Mono<Object> persisted = persistOneReactive(cnt, child, childPersistentEntity, operationContext);
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
                        if (operationContext.dialect.allowBatch()) {
                            Predicate<Object> veto = operationContext.persisted::contains;
                            RuntimePersistentEntity<Object> parentPe = getEntity((Class<Object>) cascadeOp.ctx.parent.getClass());
                            Mono<Void> op = persistManyAssociationBatchReactive(cnt, association, cascadeOp.ctx.parent, parentPe, newChildren, childPersistentEntity, veto, operationContext);
                            return op.thenReturn(entityAfterCascade);
                        } else {
                            Mono<T> res = Mono.just(entityAfterCascade);
                            for (Object child : newChildren) {
                                if (operationContext.persisted.contains(child)) {
                                    continue;
                                }
                                RuntimePersistentEntity<Object> runtimePersistentEntity = getEntity((Class<Object>) cascadeOp.ctx.parent.getClass());
                                Mono<Void> op = persistManyAssociationReactive(cnt, association, cascadeOp.ctx.parent, runtimePersistentEntity, child, childPersistentEntity, operationContext);
                                res = res.flatMap(op::thenReturn);
                            }
                            return res;
                        }
                    }
                    operationContext.persisted.addAll(newChildren);
                    return Mono.just(entityAfterCascade);
                }));

            }
        }
        return entity;
    }

    /**
     * Process after a child element has been cascaded.
     *
     * @param entity       The parent entity.
     * @param associations The association leading to the child
     * @param prevChild    The previous child value
     * @param newChild     The new child value
     * @param <T>          The entity type
     * @return The entity instance
     */
    protected <T> T afterCascadedOne(T entity, List<Association> associations, Object prevChild, Object newChild) {
        RuntimeAssociation<Object> association = (RuntimeAssociation<Object>) associations.iterator().next();
        if (associations.size() == 1) {
            if (association.isForeignKey()) {
                RuntimeAssociation<Object> inverseAssociation = (RuntimeAssociation) association.getInverseSide().orElse(null);
                if (inverseAssociation != null) {
                    //don't cast to BeanProperty<T..> here because its the inverse, so we want to set the entity onto the newChild
                    BeanProperty property = inverseAssociation.getProperty();
                    newChild = setProperty(property, newChild, entity);
                }
            }
            if (prevChild != newChild) {
                entity = setProperty((BeanProperty<T, Object>) association.getProperty(), entity, newChild);
            }
            return entity;
        } else {
            BeanProperty<T, Object> property = (BeanProperty<T, Object>) association.getProperty();
            Object innerEntity = property.get(entity);
            Object newInnerEntity = afterCascadedOne(innerEntity, associations.subList(1, associations.size()), prevChild, newChild);
            if (newInnerEntity != innerEntity) {
                innerEntity = convertAndSetWithValue(property, entity, newInnerEntity);
            }
            return (T) innerEntity;
        }
    }

    /**
     * Process after a children element has been cascaded.
     *
     * @param entity       The parent entity.
     * @param associations The association leading to the child
     * @param prevChildren The previous children value
     * @param newChildren  The new children value
     * @param <T>          The entity type
     * @return The entity instance
     */
    protected <T> T afterCascadedMany(T entity, List<Association> associations, Iterable<Object> prevChildren, List<Object> newChildren) {
        RuntimeAssociation<Object> association = (RuntimeAssociation<Object>) associations.iterator().next();
        if (associations.size() == 1) {
            for (ListIterator<Object> iterator = newChildren.listIterator(); iterator.hasNext(); ) {
                Object c = iterator.next();
                if (association.isForeignKey()) {
                    RuntimeAssociation inverseAssociation = association.getInverseSide().orElse(null);
                    if (inverseAssociation != null) {
                        BeanProperty property = inverseAssociation.getProperty();
                        Object newc = setProperty(property, c, entity);
                        if (c != newc) {
                            iterator.set(newc);
                        }
                    }
                }
            }
            if (prevChildren != newChildren) {
                entity = convertAndSetWithValue((BeanProperty<T, Object>) association.getProperty(), entity, newChildren);
            }
            return entity;
        } else {
            BeanProperty<T, Object> property = (BeanProperty<T, Object>) association.getProperty();
            Object innerEntity = property.get(entity);
            Object newInnerEntity = afterCascadedMany(innerEntity, associations.subList(1, associations.size()), prevChildren, newChildren);
            if (newInnerEntity != innerEntity) {
                innerEntity = convertAndSetWithValue(property, entity, newInnerEntity);
            }
            return (T) innerEntity;
        }
    }

    /**
     * Trigger the post load event.
     *
     * @param entity             The entity
     * @param pe                 The persistent entity
     * @param annotationMetadata The annotation metadata
     * @param <T>                The generic type
     * @return The entity, possibly modified
     */
    @SuppressWarnings("unchecked")
    protected <T> T triggerPostLoad(@NonNull T entity, RuntimePersistentEntity<T> pe, AnnotationMetadata annotationMetadata) {
        final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(pe, entity);
        entityEventRegistry.postLoad((EntityEventContext<Object>) event);
        return event.getEntity();
    }

    /**
     * Persist one operation.
     *
     * @param connection The connection
     * @param op         The operation
     * @param <T>        The entity type
     */
    protected <T> void persistOneSync(Cnt connection, EntityOperations<T> op, OperationContext operationContext) {
        try {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing SQL Insert: {}", op.debug());
            }
            boolean vetoed = op.triggerPrePersist();
            if (vetoed) {
                return;
            }
            op.cascadePre(Relation.Cascade.PERSIST, connection, operationContext);
            op.executeUpdate(this, connection);
            op.triggerPostPersist();
            op.cascadePost(Relation.Cascade.PERSIST, connection, operationContext);
        } catch (Exception e) {
            throw new DataAccessException("SQL Error executing INSERT: " + e.getMessage(), e);
        }
    }

    /**
     * Persist batch operation.
     *
     * @param connection The connection
     * @param op         The operation
     * @param <T>        The entity type
     */
    protected <T> void persistInBatch(
            Cnt connection,
            EntitiesOperations<T> op,
            OperationContext operationContext) {
        try {
            boolean allVetoed = op.triggerPrePersist();
            if (allVetoed) {
                return;
            }
            op.cascadePre(Relation.Cascade.PERSIST, connection, operationContext);

            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Batch SQL Insert: {}", op.debug());
            }
            op.executeUpdate(this, connection);
            op.triggerPostPersist();
            op.cascadePost(Relation.Cascade.PERSIST, connection, operationContext);
        } catch (Exception e) {
            throw new DataAccessException("SQL error executing INSERT: " + e.getMessage(), e);
        }
    }

    private <X, Y> X setProperty(BeanProperty<X, Y> beanProperty, X x, Y y) {
        if (beanProperty.isReadOnly()) {
            return beanProperty.withValue(x, y);
        }
        beanProperty.set(x, y);
        return x;
    }

    private <B, T> B convertAndSetWithValue(BeanProperty<B, T> beanProperty, B bean, T value) {
        Argument<T> argument = beanProperty.asArgument();
        final ArgumentConversionContext<T> context = ConversionContext.of(argument);
        T convertedValue = conversionService.convert(value, context).orElseThrow(() ->
                new ConversionErrorException(argument, context.getLastError()
                        .orElse(() -> new IllegalArgumentException("Value [" + value + "] cannot be converted to type : " + beanProperty.getType())))
        );
        if (beanProperty.isReadOnly()) {
            return beanProperty.withValue(bean, convertedValue);
        }
        beanProperty.set(bean, convertedValue);
        return bean;
    }

    /**
     * Delete one operation.
     *
     * @param connection The connection
     * @param op         The entity operation
     * @param <T>        The entity type
     */
    protected <T> void deleteOne(Cnt connection, EntityOperations<T> op) {
        op.collectAutoPopulatedPreviousValues();
        boolean vetoed = op.triggerPreRemove();
        if (vetoed) {
            // operation vetoed
            return;
        }
        try {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing SQL DELETE: {}", op.debug());
            }
            op.executeUpdate(this, connection, (entries, deleted) -> {
                if (QUERY_LOG.isTraceEnabled()) {
                    QUERY_LOG.trace("Delete operation deleted {} records", deleted);
                }
                if (op.getDbOperation().isOptimisticLock()) {
                    checkOptimisticLocking(entries, deleted);
                }
            });
            op.triggerPostRemove();
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL DELETE: " + e.getMessage(), e);
        }
    }

    /**
     * Delete batch operation.
     *
     * @param connection  The connection
     * @param op          The operation
     * @param dbOperation The db operations
     * @param <T>         The entity type
     */
    protected <T> void deleteInBatch(Cnt connection, EntitiesOperations<T> op, DBOperation dbOperation) {
        op.collectAutoPopulatedPreviousValues();
        boolean vetoed = op.triggerPreRemove();
        if (vetoed) {
            // operation vetoed
            return;
        }
        try {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Batch SQL DELETE: {}", op.debug());
            }
            op.executeUpdate(this, connection, (entries, deleted) -> {
                if (QUERY_LOG.isTraceEnabled()) {
                    QUERY_LOG.trace("Delete operation deleted {} records", deleted);
                }
                if (dbOperation.isOptimisticLock()) {
                    checkOptimisticLocking(entries, deleted);
                }
            });
            op.triggerPostRemove();
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL DELETE: " + e.getMessage(), e);
        }
    }

    /**
     * Update one operation.
     *
     * @param connection The connection
     * @param op         The operation
     * @param <T>        The entity type
     */
    protected <T> void updateOneSync(Cnt connection,
                                     EntityOperations<T> op,
                                     OperationContext operationContext) {
        op.collectAutoPopulatedPreviousValues();
        boolean vetoed = op.triggerPreUpdate();
        if (vetoed) {
            return;
        }
        op.cascadePre(Relation.Cascade.UPDATE, connection, operationContext);
        try {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing SQL UPDATE: {}", op.debug());
            }
            op.executeUpdate(this, connection, (entries, rowsUpdated) -> {
                if (QUERY_LOG.isTraceEnabled()) {
                    QUERY_LOG.trace("Update operation updated {} records", rowsUpdated);
                }
                if (op.getDbOperation().isOptimisticLock()) {
                    checkOptimisticLocking(entries, rowsUpdated);
                }
            });
            op.triggerPostUpdate();
            op.cascadePost(Relation.Cascade.UPDATE, connection, operationContext);
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL UPDATE: " + e.getMessage(), e);
        }
    }

    /**
     * Update batch operation.
     *
     * @param connection The connection
     * @param op         The operation
     * @param <T>        The entity type
     */
    protected <T> void updateInBatch(Cnt connection,
                                     EntitiesOperations<T> op,
                                     OperationContext operationContext) {
        op.collectAutoPopulatedPreviousValues();
        op.triggerPreUpdate();
        try {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Batch SQL Update: {}", op.debug());
            }
            op.cascadePre(Relation.Cascade.UPDATE, connection, operationContext);
            op.executeUpdate(this, connection, (expected, updated) -> {
                if (QUERY_LOG.isTraceEnabled()) {
                    QUERY_LOG.trace("Update batch operation updated {} records", updated);
                }
                if (op.getDbOperation().isOptimisticLock()) {
                    checkOptimisticLocking(expected, updated);
                }
            });
            op.cascadePost(Relation.Cascade.UPDATE, connection, operationContext);
            op.triggerPostUpdate();
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL UPDATE: " + e.getMessage(), e);
        }
    }

    /**
     * Used to define the index whether it is 1 based (JDBC) or 0 based (R2DBC).
     *
     * @param i The index to shift
     * @return the index
     */
    public int shiftIndex(int i) {
        return i + 1;
    }

    /**
     * Obtain an ID reader for the given object.
     *
     * @param o The object
     * @return The ID reader
     */
    @NonNull
    protected final RuntimePersistentProperty<Object> getIdReader(@NonNull Object o) {
        Class<Object> type = (Class<Object>) o.getClass();
        RuntimePersistentProperty beanProperty = idReaders.get(type);
        if (beanProperty == null) {
            RuntimePersistentEntity<Object> entity = getEntity(type);
            RuntimePersistentProperty<Object> identity = entity.getIdentity();
            if (identity == null) {
                throw new DataAccessException("Entity has no ID: " + entity.getName());
            }
            beanProperty = identity;
            idReaders.put(type, beanProperty);
        }
        return beanProperty;
    }

    /**
     * Cascade on the entity instance and collect cascade operations.
     *
     * @param annotationMetadata The annotationMetadata
     * @param repositoryType     The repositoryType
     * @param fkOnly             Is FK only
     * @param cascadeType        The cascadeType
     * @param ctx                The cascade context
     * @param persistentEntity   The persistent entity
     * @param entity             The entity instance
     * @param cascadeOps         The cascade operations
     * @param <T>                The entity type
     */
    protected <T> void cascade(AnnotationMetadata annotationMetadata, Class<?> repositoryType,
                               boolean fkOnly,
                               Relation.Cascade cascadeType,
                               CascadeContext ctx,
                               RuntimePersistentEntity<T> persistentEntity,
                               T entity,
                               List<CascadeOp> cascadeOps) {
        for (RuntimeAssociation<T> association : persistentEntity.getAssociations()) {
            BeanProperty<T, Object> beanProperty = (BeanProperty<T, Object>) association.getProperty();
            Object child = beanProperty.get(entity);
            if (child == null) {
                continue;
            }
            if (association instanceof Embedded) {
                cascade(annotationMetadata, repositoryType, fkOnly, cascadeType, ctx.embedded(association),
                        (RuntimePersistentEntity) association.getAssociatedEntity(),
                        child,
                        cascadeOps);
                continue;
            }
            if (association.doesCascade(cascadeType) && (fkOnly || !association.isForeignKey())) {
                if (association.getInverseSide().map(assoc -> ctx.rootAssociations.contains(assoc) || ctx.associations.contains(assoc)).orElse(false)) {
                    continue;
                }
                final RuntimePersistentEntity<Object> associatedEntity = (RuntimePersistentEntity<Object>) association.getAssociatedEntity();
                switch (association.getKind()) {
                    case ONE_TO_ONE:
                    case MANY_TO_ONE:
                        cascadeOps.add(new CascadeOneOp(annotationMetadata, repositoryType, ctx.relation(association), cascadeType, associatedEntity, child));
                        continue;
                    case ONE_TO_MANY:
                    case MANY_TO_MANY:
                        final RuntimeAssociation inverse = association.getInverseSide().orElse(null);
                        Iterable<Object> children = (Iterable<Object>) association.getProperty().get(entity);
                        if (!children.iterator().hasNext()) {
                            continue;
                        }
                        if (inverse != null && inverse.getKind() == Relation.Kind.MANY_TO_ONE) {
                            List<Object> entities = new ArrayList<>(CollectionUtils.iterableToList(children));
                            for (ListIterator<Object> iterator = entities.listIterator(); iterator.hasNext(); ) {
                                Object c = iterator.next();
                                final BeanProperty property = inverse.getProperty();
                                c = setProperty(property, c, entity);
                                iterator.set(c);
                            }
                            children = entities;
                        }
                        cascadeOps.add(new CascadeManyOp(annotationMetadata, repositoryType, ctx.relation(association), cascadeType, associatedEntity, children));
                        continue;
                    default:
                        throw new IllegalArgumentException("Cannot cascade for relation: " + association.getKind());
                }
            }
        }
    }

    protected Stream<Map.Entry<PersistentProperty, Object>> idPropertiesWithValues(PersistentProperty property, Object value) {
        Object propertyValue = ((RuntimePersistentProperty) property).getProperty().get(value);
        if (property instanceof Embedded) {
            Embedded embedded = (Embedded) property;
            PersistentEntity embeddedEntity = embedded.getAssociatedEntity();
            return embeddedEntity.getPersistentProperties()
                    .stream()
                    .flatMap(prop -> idPropertiesWithValues(prop, propertyValue));
        } else if (property instanceof Association) {
            Association association = (Association) property;
            if (association.isForeignKey()) {
                return Stream.empty();
            }
            PersistentEntity associatedEntity = association.getAssociatedEntity();
            PersistentProperty identity = associatedEntity.getIdentity();
            if (identity == null) {
                throw new IllegalStateException("Identity cannot be missing for: " + associatedEntity);
            }
            return idPropertiesWithValues(identity, propertyValue);
        }
        return Stream.of(new AbstractMap.SimpleEntry<>(property, propertyValue));
    }

    /**
     * Does supports batch for update queries.
     *
     * @param persistentEntity The persistent entity
     * @param dialect          The dialect
     * @return true if supported
     */
    protected boolean isSupportsBatchInsert(PersistentEntity persistentEntity, Dialect dialect) {
        switch (dialect) {
            case SQL_SERVER:
                return false;
            case MYSQL:
            case ORACLE:
                if (persistentEntity.getIdentity() != null) {
                    // Oracle and MySql doesn't support a batch with returning generated ID: "DML Returning cannot be batched"
                    return !persistentEntity.getIdentity().isGenerated();
                }
                return false;
            default:
                return true;
        }
    }

    /**
     * Does supports batch for update queries.
     *
     * @param persistentEntity The persistent entity
     * @param dialect          The dialect
     * @return true if supported
     */
    protected boolean isSupportsBatchUpdate(PersistentEntity persistentEntity, Dialect dialect) {
        return true;
    }

    /**
     * Does supports batch for delete queries.
     *
     * @param persistentEntity The persistent entity
     * @param dialect          The dialect
     * @return true if supported
     */
    protected boolean isSupportsBatchDelete(PersistentEntity persistentEntity, Dialect dialect) {
        return true;
    }

    /**
     * Compare the expected modifications and the received rows count. If not equals throw {@link OptimisticLockException}.
     *
     * @param expected The expected value
     * @param received THe received value
     */
    protected void checkOptimisticLocking(int expected, int received) {
        if (received != expected) {
            throw new OptimisticLockException("Execute update returned unexpected row count. Expected: " + expected + " got: " + received);
        }
    }

    /**
     * Check if joined associated are all single ended (Can produce only one result).
     *
     * @param rootPersistentEntity The root entity
     * @param joinFetchPaths       The join paths
     * @return true if there are no "many" joins
     */
    protected boolean isOnlySingleEndedJoins(RuntimePersistentEntity<?> rootPersistentEntity, Set<JoinPath> joinFetchPaths) {
        boolean onlySingleEndedJoins = joinFetchPaths.isEmpty() || joinFetchPaths.stream()
                .flatMap(jp -> {
                    PersistentPropertyPath propertyPath = rootPersistentEntity.getPropertyPath(jp.getPath());
                    if (propertyPath == null) {
                        return Stream.empty();
                    }
                    if (propertyPath.getProperty() instanceof Association) {
                        return Stream.concat(propertyPath.getAssociations().stream(), Stream.of((Association) propertyPath.getProperty()));
                    }
                    return propertyPath.getAssociations().stream();
                })
                .allMatch(association -> association.getKind() == Relation.Kind.EMBEDDED || association.getKind().isSingleEnded());
        return onlySingleEndedJoins;
    }

    private static List<Association> associated(List<Association> associations, Association association) {
        if (associations == null) {
            return Collections.singletonList(association);
        }
        List<Association> newAssociations = new ArrayList<>(associations.size() + 1);
        newAssociations.addAll(associations);
        newAssociations.add(association);
        return newAssociations;
    }

    @Override
    public Object convert(Cnt connection, Object value, RuntimePersistentProperty<?> property) {
        AttributeConverter<Object, Object> converter = property.getConverter();
        if (converter != null) {
            return converter.convertToPersistedValue(value, createTypeConversionContext(connection, property, property.getArgument()));
        }
        return value;
    }

    @Override
    public Object convert(Class<?> converterClass, Cnt connection, Object value, @Nullable Argument<?> argument) {
        if (converterClass == null) {
            return value;
        }
        AttributeConverter<Object, Object> converter = attributeConverterRegistry.getConverter(converterClass);
        ConversionContext conversionContext = createTypeConversionContext(connection, null, argument);
        return converter.convertToPersistedValue(value, conversionContext);
    }

    /**
     * Creates implementation specific conversion context.
     *
     * @param connection The connection
     * @param property   The property
     * @param argument   The argument
     * @return new {@link ConversionContext}
     */
    protected abstract ConversionContext createTypeConversionContext(Cnt connection,
                                                                     @Nullable RuntimePersistentProperty<?> property,
                                                                     @Nullable Argument<?> argument);

    /**
     * Simple function interface without return type.
     *
     * @param <In>  The input type
     * @param <Exc> The exception type
     */
    protected interface DBOperation1<In, Exc extends Exception> {

        void process(In in) throws Exc;

    }

    /**
     * Simple function interface with two inputs and without return type.
     *
     * @param <In1> The input 1 type
     * @param <In2> The input 2 type
     * @param <Exc> The exception type
     */
    protected interface DBOperation2<In1, In2, Exc extends Exception> {

        void process(In1 in1, In2 in2) throws Exc;

    }

    /**
     * Functional interface used to supply a statement.
     *
     * @param <PS> The prepared statement type
     */
    @FunctionalInterface
    protected interface StatementSupplier<PS> {
        PS create(String ps) throws Exception;
    }

    /**
     * The entity operations container.
     *
     * @param <T> The entity type
     */
    protected abstract class SyncEntityOperations<T> extends EntityOperations<T> {

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         */
        protected SyncEntityOperations(RuntimePersistentEntity<T> persistentEntity) {
            super(persistentEntity);
        }

        public abstract T getEntity();

    }

    /**
     * The entity operations container.
     *
     * @param <T> The entity type
     */
    protected abstract class ReactiveEntityOperations<T> extends EntityOperations<T> {

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         */
        protected ReactiveEntityOperations(RuntimePersistentEntity<T> persistentEntity) {
            super(persistentEntity);
        }

        public abstract Mono<T> getEntity();

    }

    /**
     * The entities operations container.
     *
     * @param <T> The entity type
     */
    protected abstract class SyncEntitiesOperations<T> extends EntitiesOperations<T> {

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         */
        protected SyncEntitiesOperations(RuntimePersistentEntity<T> persistentEntity) {
            super(persistentEntity);
        }

        public abstract List<T> getEntities();
    }

    /**
     * The entities operations container.
     *
     * @param <T> The entity type
     */
    protected abstract class ReactiveEntitiesOperations<T> extends EntitiesOperations<T> {

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         */
        protected ReactiveEntitiesOperations(RuntimePersistentEntity<T> persistentEntity) {
            super(persistentEntity);
        }

        public abstract Flux<T> getEntities();
    }

    /**
     * The entity operations container.
     *
     * @param <T> The entity type
     */
    protected abstract class EntityOperations<T> extends BaseOperations<T> {

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         */
        protected EntityOperations(RuntimePersistentEntity<T> persistentEntity) {
            super(persistentEntity);
        }

    }

    /**
     * The entities operations container.
     *
     * @param <T> The entity type
     */
    protected abstract class EntitiesOperations<T> extends BaseOperations<T> {

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         */
        protected EntitiesOperations(RuntimePersistentEntity<T> persistentEntity) {
            super(persistentEntity);
        }

    }

    /**
     * The base entity operations class.
     *
     * @param <T> The entity type
     */
    protected abstract class BaseOperations<T> {

        protected final RuntimePersistentEntity<T> persistentEntity;

        protected BaseOperations(RuntimePersistentEntity<T> persistentEntity) {
            this.persistentEntity = persistentEntity;
        }

        protected abstract DBOperation getDbOperation();

        protected abstract String debug();

        /**
         * Cascade pre operation.
         *
         * @param cascadeType The cascade type
         */
        protected abstract void cascadePre(Relation.Cascade cascadeType, Cnt connection, OperationContext operationContext);

        /**
         * Cascade post operation.
         *
         * @param cascadeType The cascade type
         * @param cnt         The connection
         */
        protected abstract void cascadePost(Relation.Cascade cascadeType, Cnt cnt, OperationContext operationContext);

        /**
         * Collect auto populated values before pre-triggers modifies them.
         */
        protected abstract void collectAutoPopulatedPreviousValues();

        /**
         * Execute update and process entities modified and rows executed.
         *
         * @param context    The context
         * @param connection The connection
         * @param fn         The affected rows consumer
         * @throws Exc The exception
         */
        protected abstract void executeUpdate(OpContext<Cnt, PS> context, Cnt connection, DBOperation2<Integer, Integer, Exc> fn) throws Exc;

        /**
         * Execute update.
         *
         * @param context    The context
         * @param connection The connection
         * @throws Exc The exception
         */
        protected abstract void executeUpdate(OpContext<Cnt, PS> context, Cnt connection) throws Exc;

        /**
         * Veto an entity.
         *
         * @param predicate The veto predicate
         */
        protected abstract void veto(Predicate<T> predicate);

        /**
         * Update entity id.
         *
         * @param identity The identity property.
         * @param entity   The entity instance
         * @param id       The id instance
         * @return The entity instance
         */
        protected T updateEntityId(BeanProperty<T, Object> identity, T entity, Object id) {
            if (id == null) {
                return entity;
            }
            if (identity.getType().isInstance(id)) {
                return setProperty(identity, entity, id);
            }
            return convertAndSetWithValue(identity, entity, id);
        }

        /**
         * Trigger the pre persist event.
         *
         * @return true if operation was vetoed
         */
        protected boolean triggerPrePersist() {
            if (!persistentEntity.hasPrePersistEventListeners()) {
                return false;
            }
            return triggerPre(entityEventRegistry::prePersist);
        }

        /**
         * Trigger the pre update event.
         *
         * @return true if operation was vetoed
         */
        protected boolean triggerPreUpdate() {
            if (!persistentEntity.hasPreUpdateEventListeners()) {
                return false;
            }
            return triggerPre(entityEventRegistry::preUpdate);
        }

        /**
         * Trigger the pre remove event.
         *
         * @return true if operation was vetoed
         */
        protected boolean triggerPreRemove() {
            if (!persistentEntity.hasPreRemoveEventListeners()) {
                return false;
            }
            return triggerPre(entityEventRegistry::preRemove);
        }

        /**
         * Trigger the post update event.
         */
        protected void triggerPostUpdate() {
            if (!persistentEntity.hasPostUpdateEventListeners()) {
                return;
            }
            triggerPost(entityEventRegistry::postUpdate);
        }

        /**
         * Trigger the post remove event.
         */
        protected void triggerPostRemove() {
            if (!persistentEntity.hasPostRemoveEventListeners()) {
                return;
            }
            triggerPost(entityEventRegistry::postRemove);
        }

        /**
         * Trigger the post persist event.
         */
        protected void triggerPostPersist() {
            if (!persistentEntity.hasPostPersistEventListeners()) {
                return;
            }
            triggerPost(entityEventRegistry::postPersist);
        }

        /**
         * Trigger pre-actions on {@link EntityEventContext}.
         *
         * @param fn The entity context function
         * @return true if operation was vetoed
         */
        protected abstract boolean triggerPre(Function<EntityEventContext<Object>, Boolean> fn);

        /**
         * Trigger post-actions on {@link EntityEventContext}.
         *
         * @param fn The entity context function
         */
        protected abstract void triggerPost(Consumer<EntityEventContext<Object>> fn);

    }

    /**
     * The base cascade operation.
     */
    @SuppressWarnings("VisibilityModifier")
    protected abstract static class CascadeOp {

        public final AnnotationMetadata annotationMetadata;
        public final Class<?> repositoryType;
        public final CascadeContext ctx;
        public final Relation.Cascade cascadeType;
        public final RuntimePersistentEntity<Object> childPersistentEntity;

        CascadeOp(AnnotationMetadata annotationMetadata,
                  Class<?> repositoryType,
                  CascadeContext ctx,
                  Relation.Cascade cascadeType,
                  RuntimePersistentEntity<Object> childPersistentEntity) {
            this.annotationMetadata = annotationMetadata;
            this.repositoryType = repositoryType;
            this.ctx = ctx;
            this.cascadeType = cascadeType;
            this.childPersistentEntity = childPersistentEntity;
        }
    }

    /**
     * The cascade operation of one entity.
     */
    @SuppressWarnings("VisibilityModifier")
    protected static final class CascadeOneOp extends CascadeOp {

        public final Object child;

        CascadeOneOp(AnnotationMetadata annotationMetadata, Class<?> repositoryType,
                     CascadeContext ctx, Relation.Cascade cascadeType, RuntimePersistentEntity<Object> childPersistentEntity, Object child) {
            super(annotationMetadata, repositoryType, ctx, cascadeType, childPersistentEntity);
            this.child = child;
        }
    }

    /**
     * The cascade operation of multiple entities - @Many mappings.
     */
    @SuppressWarnings("VisibilityModifier")
    protected static final class CascadeManyOp extends CascadeOp {

        public final Iterable<Object> children;

        CascadeManyOp(AnnotationMetadata annotationMetadata, Class<?> repositoryType,
                      CascadeContext ctx, Relation.Cascade cascadeType, RuntimePersistentEntity<Object> childPersistentEntity,
                      Iterable<Object> children) {
            super(annotationMetadata, repositoryType, ctx, cascadeType, childPersistentEntity);
            this.children = children;
        }
    }

    /**
     * The cascade context.
     */
    @SuppressWarnings("VisibilityModifier")
    protected static final class CascadeContext {

        /**
         * The associations leading to the parent.
         */
        public final List<Association> rootAssociations;
        /**
         * The parent instance that is being cascaded.
         */
        public final Object parent;
        /**
         * The associations leading to the cascaded instance.
         */
        public final List<Association> associations;

        /**
         * Create a new instance.
         *
         * @param rootAssociations The root associations.
         * @param parent           The parent
         * @param associations     The associations
         */
        CascadeContext(List<Association> rootAssociations, Object parent, List<Association> associations) {
            this.rootAssociations = rootAssociations;
            this.parent = parent;
            this.associations = associations;
        }

        public static CascadeContext of(List<Association> rootAssociations, Object parent) {
            return new CascadeContext(rootAssociations, parent, Collections.emptyList());
        }

        /**
         * Cascade embedded association.
         *
         * @param association The embedded association
         * @return The context
         */
        CascadeContext embedded(Association association) {
            return new CascadeContext(rootAssociations, parent, associated(associations, association));
        }

        /**
         * Cascade relation association.
         *
         * @param association The relation association
         * @return The context
         */
        CascadeContext relation(Association association) {
            return new CascadeContext(rootAssociations, parent, associated(associations, association));
        }

        /**
         * Get last association.
         *
         * @return last association
         */
        public Association getAssociation() {
            return CollectionUtils.last(associations);
        }

    }

    protected static class OperationContext {
        public final AnnotationMetadata annotationMetadata;
        public final Class<?> repositoryType;
        public final List<Association> associations = Collections.emptyList();
        public final Set<Object> persisted = new HashSet<>(5);
        public final Dialect dialect;

        public OperationContext(AnnotationMetadata annotationMetadata, Class<?> repositoryType, Dialect dialect) {
            this.annotationMetadata = annotationMetadata;
            this.repositoryType = repositoryType;
            this.dialect = dialect;
        }
    }

}
