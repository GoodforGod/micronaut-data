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
package io.micronaut.data.r2dbc.operations;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.AnnotationMetadataProvider;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.attr.AttributeHolder;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.exceptions.NonUniqueResultException;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.query.JoinPath;
import io.micronaut.data.model.query.builder.sql.SqlQueryBuilder;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.DeleteBatchOperation;
import io.micronaut.data.model.runtime.DeleteOperation;
import io.micronaut.data.model.runtime.EntityOperation;
import io.micronaut.data.model.runtime.InsertBatchOperation;
import io.micronaut.data.model.runtime.InsertOperation;
import io.micronaut.data.model.runtime.PagedQuery;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.UpdateBatchOperation;
import io.micronaut.data.model.runtime.UpdateOperation;
import io.micronaut.data.operations.async.AsyncRepositoryOperations;
import io.micronaut.data.r2dbc.annotation.R2dbcRepository;
import io.micronaut.data.r2dbc.convert.R2dbcConversionContext;
import io.micronaut.data.r2dbc.mapper.ColumnIndexR2dbcResultReader;
import io.micronaut.data.r2dbc.mapper.ColumnNameR2dbcResultReader;
import io.micronaut.data.r2dbc.mapper.R2dbcQueryStatement;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.convert.RuntimePersistentPropertyConversionContext;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;
import io.micronaut.data.runtime.mapper.DTOMapper;
import io.micronaut.data.runtime.mapper.TypeMapper;
import io.micronaut.data.runtime.mapper.sql.SqlDTOMapper;
import io.micronaut.data.runtime.mapper.sql.SqlResultEntityTypeMapper;
import io.micronaut.data.runtime.operations.AsyncFromReactiveAsyncRepositoryOperation;
import io.micronaut.data.runtime.operations.internal.AbstractSqlRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.DBOperation;
import io.micronaut.data.runtime.operations.internal.OpContext;
import io.micronaut.data.runtime.operations.internal.StoredQuerySqlOperation;
import io.micronaut.data.runtime.operations.internal.StoredSqlOperation;
import io.micronaut.data.runtime.support.AbstractConversionContext;
import io.micronaut.http.codec.MediaTypeCodec;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.annotation.TransactionalAdvice;
import io.micronaut.transaction.exceptions.NoTransactionException;
import io.micronaut.transaction.exceptions.TransactionSystemException;
import io.micronaut.transaction.exceptions.TransactionUsageException;
import io.micronaut.transaction.interceptor.DefaultTransactionAttribute;
import io.micronaut.transaction.reactive.ReactiveTransactionOperations;
import io.micronaut.transaction.reactive.ReactiveTransactionStatus;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import jakarta.inject.Named;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines an implementation of Micronaut Data's core interfaces for R2DBC.
 *
 * @author graemerocher
 * @author Denis Stepanov
 * @since 1.0.0
 */
@EachBean(ConnectionFactory.class)
@Internal
final class DefaultR2dbcRepositoryOperations extends AbstractSqlRepositoryOperations<Connection, Row, Statement, RuntimeException> implements BlockingReactorRepositoryOperations, R2dbcRepositoryOperations, R2dbcOperations, ReactiveTransactionOperations<Connection> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultR2dbcRepositoryOperations.class);
    private final ConnectionFactory connectionFactory;
    private final ReactorReactiveRepositoryOperations reactiveOperations;
    private final String dataSourceName;
    private ExecutorService executorService;
    private AsyncRepositoryOperations asyncRepositoryOperations;

    /**
     * Default constructor.
     *
     * @param dataSourceName             The data source name
     * @param connectionFactory          The associated connection factory
     * @param mediaTypeCodecList         The media type codec list
     * @param dateTimeProvider           The date time provider
     * @param runtimeEntityRegistry      The runtime entity registry
     * @param applicationContext         The bean context
     * @param executorService            The executor
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     */
    @Internal
    protected DefaultR2dbcRepositoryOperations(
            @Parameter String dataSourceName,
            ConnectionFactory connectionFactory,
            List<MediaTypeCodec> mediaTypeCodecList,
            @NonNull DateTimeProvider<Object> dateTimeProvider,
            RuntimeEntityRegistry runtimeEntityRegistry,
            ApplicationContext applicationContext,
            @Nullable @Named("io") ExecutorService executorService,
            DataConversionService<?> conversionService,
            AttributeConverterRegistry attributeConverterRegistry) {
        super(
                dataSourceName,
                new ColumnNameR2dbcResultReader(conversionService),
                new ColumnIndexR2dbcResultReader(conversionService),
                new R2dbcQueryStatement(conversionService),
                mediaTypeCodecList,
                dateTimeProvider,
                runtimeEntityRegistry,
                applicationContext,
                conversionService, attributeConverterRegistry);
        this.connectionFactory = connectionFactory;
        this.executorService = executorService;
        this.reactiveOperations = new DefaultR2dbcReactiveRepositoryOperations();
        this.dataSourceName = dataSourceName;
    }

    private <T> Mono<T> cascadeEntity(T en, RuntimePersistentEntity<T> persistentEntity,
                                      boolean isPost, Relation.Cascade cascadeType, Connection connection,
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
                    DBOperation childSqlPersistOperation = resolveEntityInsert(operationContext.annotationMetadata, operationContext.repositoryType, child.getClass(), childPersistentEntity);
                    R2dbcEntityOperations<Object> op = new R2dbcEntityOperations<>(childSqlPersistOperation, childPersistentEntity, child, true);
                    persistOneSync(connection, op, operationContext);
                    entity = entity.flatMap(e -> op.data.map(childData -> afterCascadedOne(e, cascadeOp.ctx.associations, child, childData.entity)));
                    childMono = op.data.map(childData -> childData.entity);
                } else if (hasId && (cascadeType == Relation.Cascade.UPDATE)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading MERGE for '{}' ({}) association: '{}'", persistentEntity.getName(),
                                persistentEntity.getIdentity().getProperty().get(en), cascadeOp.ctx.associations);
                    }
                    DBOperation childSqlUpdateOperation = resolveEntityUpdate(operationContext.annotationMetadata, operationContext.repositoryType, child.getClass(), childPersistentEntity);
                    R2dbcEntityOperations<Object> op = new R2dbcEntityOperations<>(childPersistentEntity, child, childSqlUpdateOperation);
                    updateOneSync(connection, op, operationContext);
                    entity = entity.flatMap(e -> op.data.map(childData -> afterCascadedOne(e, cascadeOp.ctx.associations, child, childData.entity)));
                    childMono = op.data.map(childData -> childData.entity);
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
                        DBOperation dbInsertOperation = resolveSqlInsertAssociation(operationContext.repositoryType, operationContext.dialect, (RuntimeAssociation) association, entity1, e);
                        R2dbcEntityOperations<Object> assocEntityOp = new R2dbcEntityOperations<>(childPersistentEntity, c, dbInsertOperation);
                        try {
                            assocEntityOp.executeUpdate(this, connection);
                        } catch (Exception e1) {
                            throw new DataAccessException("SQL error executing INSERT: " + e1.getMessage(), e1);
                        }
                        return assocEntityOp.getEntity().thenReturn(e);
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
                    DBOperation childSqlUpdateOperation = null;
                    DBOperation childSqlInsertOperation = null;
                    Flux<Object> childrenFlux = Flux.empty();
                    for (Object child : cascadeManyOp.children) {
                        if (operationContext.persisted.contains(child)) {
                            continue;
                        }

                        R2dbcEntityOperations<Object> op;

                        if (childPersistentEntity.getIdentity().getProperty().get(child) == null) {
                            if (childSqlInsertOperation == null) {
                                childSqlInsertOperation = resolveEntityInsert(operationContext.annotationMetadata, operationContext.repositoryType, childPersistentEntity.getIntrospection().getBeanType(), childPersistentEntity);
                            }
                            op = new R2dbcEntityOperations<>(childSqlInsertOperation, childPersistentEntity, child, true);
                            persistOneSync(connection, op, operationContext);
                        } else {
                            if (childSqlUpdateOperation == null) {
                                childSqlUpdateOperation = resolveEntityUpdate(operationContext.annotationMetadata, operationContext.repositoryType, childPersistentEntity.getIntrospection().getBeanType(), childPersistentEntity);
                            }
                            op = new R2dbcEntityOperations<>(childSqlUpdateOperation, childPersistentEntity, child, true);
                            updateOneSync(connection, op, operationContext);
                        }

                        childrenFlux = childrenFlux.concatWith(op.getEntity());
                    }
                    children = childrenFlux.collectList();
                } else if (cascadeType == Relation.Cascade.PERSIST) {
                    DBOperation childSqlPersistOperation = resolveEntityInsert(
                            operationContext.annotationMetadata,
                            operationContext.repositoryType,
                            childPersistentEntity.getIntrospection().getBeanType(),
                            childPersistentEntity
                    );
                    if (isSupportsBatchInsert(persistentEntity, operationContext.dialect)) {
                        R2dbcEntitiesOperations<Object> op = new R2dbcEntitiesOperations<>(childSqlPersistOperation, childPersistentEntity, cascadeManyOp.children, true);
                        op.veto(operationContext.persisted::contains);
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        op.veto(e -> identity.getProperty().get(e) != null);

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Cascading PERSIST for '{}' association: '{}'", persistentEntity.getName(), cascadeOp.ctx.associations);
                        }

                        persistInBatch(connection, op, operationContext);

                        children = op.getEntities().collectList();
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

                            R2dbcEntityOperations<Object> op = new R2dbcEntityOperations<>(childSqlPersistOperation, childPersistentEntity, child, true);

                            persistOneSync(connection, op, operationContext);

                            childrenFlux = childrenFlux.concatWith(op.getEntity());
                        }
                        children = childrenFlux.collectList();
                    }
                } else {
                    continue;
                }
                entity = entity.flatMap(e -> children.flatMap(newChildren -> {
                    T e2 = afterCascadedMany(e, cascadeOp.ctx.associations, cascadeManyOp.children, newChildren);
                    RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                    if (SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {
                        if (operationContext.dialect.allowBatch()) {
                            RuntimePersistentEntity<Object> entity1 = getEntity((Class<Object>) cascadeOp.ctx.parent.getClass());
                            DBOperation dbInsertOperation = resolveSqlInsertAssociation(operationContext.repositoryType, operationContext.dialect, (RuntimeAssociation) association, entity1, cascadeOp.ctx.parent);
                            R2dbcEntitiesOperations<Object> assocEntitiesOp = new R2dbcEntitiesOperations<>(childPersistentEntity, newChildren, dbInsertOperation);
                            assocEntitiesOp.veto(operationContext.persisted::contains);
                            try {
                                assocEntitiesOp.executeUpdate(this, connection);
                            } catch (Exception e1) {
                                throw new DataAccessException("SQL error executing INSERT: " + e1.getMessage(), e1);
                            }
                            return assocEntitiesOp.entities.collectList().thenReturn(e2);
                        } else {
                            Mono<T> res = Mono.just(e2);
                            for (Object child : newChildren) {
                                if (operationContext.persisted.contains(child)) {
                                    continue;
                                }
                                RuntimePersistentEntity<Object> entity1 = getEntity((Class<Object>) cascadeOp.ctx.parent.getClass());
                                DBOperation dbInsertOperation = resolveSqlInsertAssociation(operationContext.repositoryType, operationContext.dialect, (RuntimeAssociation) association, entity1, cascadeOp.ctx.parent);
                                R2dbcEntityOperations<Object> assocEntityOp = new R2dbcEntityOperations<>(childPersistentEntity, child, dbInsertOperation);
                                try {
                                    assocEntityOp.executeUpdate(this, connection);
                                } catch (Exception e1) {
                                    throw new DataAccessException("SQL error executing INSERT: " + e1.getMessage(), e1);
                                }
                                res = res.flatMap(e3 -> assocEntityOp.getEntity().thenReturn(e3));
                            }
                            return res;
                        }
                    }
                    operationContext.persisted.addAll(newChildren);
                    return Mono.just(e2);
                }));

            }
        }
        return entity;
    }

    @Override
    protected ConversionContext createTypeConversionContext(Connection connection,
                                                            RuntimePersistentProperty<?> property,
                                                            Argument<?> argument) {
        Objects.requireNonNull(connection);
        if (property != null) {
            return new RuntimePersistentPropertyR2dbcCC(connection, property);
        }
        if (argument != null) {
            return new ArgumentR2dbcCC(connection, argument);
        }
        return new R2dbcConversionContextImpl(connection);
    }

    private Mono<Integer> sum(Stream<Mono<Integer>> stream) {
        return stream.reduce((m1, m2) -> m1.zipWith(m2).map(t -> t.getT1() + t.getT2())).orElse(Mono.empty());
    }

    private <T> Flux<T> concatMono(Stream<Mono<T>> stream) {
        return Flux.concat(stream.collect(Collectors.toList()));
    }

    @Override
    public int shiftIndex(int i) {
        return i;
    }

    @NonNull
    @Override
    public ReactorReactiveRepositoryOperations reactive() {
        return reactiveOperations;
    }

    @NonNull
    @Override
    public AsyncRepositoryOperations async() {
        if (asyncRepositoryOperations == null) {
            if (executorService == null) {
                executorService = Executors.newCachedThreadPool();
            }
            asyncRepositoryOperations = new AsyncFromReactiveAsyncRepositoryOperation(reactiveOperations, executorService);
        }
        return asyncRepositoryOperations;
    }

    @NonNull
    @Override
    public ConnectionFactory connectionFactory() {
        return connectionFactory;
    }

    @NonNull
    @Override
    public <T> Publisher<T> withConnection(@NonNull Function<Connection, Publisher<T>> handler) {
        Objects.requireNonNull(handler, "Handler cannot be null");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating a new Connection for DataSource: " + dataSourceName);
        }
        return Flux.usingWhen(connectionFactory.create(), handler, (connection -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing Connection for DataSource: " + dataSourceName);
            }
            return connection.close();
        }));
    }

    private IsolationLevel getIsolationLevel(TransactionDefinition definition) {
        TransactionDefinition.Isolation isolationLevel = definition.getIsolationLevel();
        switch (isolationLevel) {
            case READ_COMMITTED:
                return IsolationLevel.READ_COMMITTED;
            case READ_UNCOMMITTED:
                return IsolationLevel.READ_UNCOMMITTED;
            case REPEATABLE_READ:
                return IsolationLevel.REPEATABLE_READ;
            case SERIALIZABLE:
                return IsolationLevel.SERIALIZABLE;
            default:
                return null;
        }
    }

    @NonNull
    @Override
    public <T> Publisher<T> withTransaction(
            @NonNull ReactiveTransactionStatus<Connection> status,
            @NonNull ReactiveTransactionOperations.TransactionalCallback<Connection, T> handler) {
        Objects.requireNonNull(status, "Transaction status cannot be null");
        Objects.requireNonNull(handler, "Callback handler cannot be null");
        return Flux.defer(() -> {
            try {
                return handler.doInTransaction(status);
            } catch (Exception e) {
                return Flux.error(new TransactionSystemException("Error invoking doInTransaction handler: " + e.getMessage(), e));
            }
        }).contextWrite(context -> context.put(ReactiveTransactionStatus.STATUS, status));
    }

    @Override
    public @NonNull
    <T> Flux<T> withTransaction(
            @NonNull TransactionDefinition definition,
            @NonNull ReactiveTransactionOperations.TransactionalCallback<Connection, T> handler) {
        Objects.requireNonNull(definition, "Transaction definition cannot be null");
        Objects.requireNonNull(handler, "Callback handler cannot be null");

        return Flux.deferContextual(contextView -> {
            Object o = contextView.getOrDefault(ReactiveTransactionStatus.STATUS, null);
            TransactionDefinition.Propagation propagationBehavior = definition.getPropagationBehavior();
            if (o instanceof ReactiveTransactionStatus) {
                // existing transaction, use it
                if (propagationBehavior == TransactionDefinition.Propagation.NOT_SUPPORTED || propagationBehavior == TransactionDefinition.Propagation.NEVER) {
                    return Flux.error(new TransactionUsageException("Found an existing transaction but propagation behaviour doesn't support it: " + propagationBehavior));
                }
                try {
                    return handler.doInTransaction(((ReactiveTransactionStatus<Connection>) o));
                } catch (Exception e) {
                    return Flux.error(new TransactionSystemException("Error invoking doInTransaction handler: " + e.getMessage(), e));
                }
            } else {

                if (propagationBehavior == TransactionDefinition.Propagation.MANDATORY) {
                    return Flux.error(new NoTransactionException("Expected an existing transaction, but none was found in the Reactive context."));
                }
                return withConnection(connection -> {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Transaction Begin for DataSource: {}", dataSourceName);
                            }
                            DefaultReactiveTransactionStatus status = new DefaultReactiveTransactionStatus(connection, true);
                            Mono<Boolean> resourceSupplier;
                            if (definition.getIsolationLevel() != TransactionDefinition.DEFAULT.getIsolationLevel()) {
                                IsolationLevel isolationLevel = getIsolationLevel(definition);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Setting Isolation Level ({}) for Transaction on DataSource: {}", isolationLevel, dataSourceName);
                                }
                                if (isolationLevel != null) {
                                    resourceSupplier = Flux.from(connection.setTransactionIsolationLevel(isolationLevel))
                                            .thenMany(connection.beginTransaction())
                                            .hasElements();
                                } else {
                                    resourceSupplier = Flux.from(connection.beginTransaction()).hasElements();
                                }
                            } else {
                                resourceSupplier = Flux.from(connection.beginTransaction()).hasElements();
                            }

                            return Flux.usingWhen(resourceSupplier,
                                    (b) -> {
                                        try {
                                            return Flux.from(handler.doInTransaction(status)).contextWrite(context ->
                                                    context.put(ReactiveTransactionStatus.STATUS, status)
                                                            .put(ReactiveTransactionStatus.ATTRIBUTE, definition)
                                            );
                                        } catch (Exception e) {
                                            return Mono.error(new TransactionSystemException("Error invoking doInTransaction handler: " + e.getMessage(), e));
                                        }
                                    },
                                    (b) -> doCommit(status),
                                    (b, throwable) -> {
                                        if (LOG.isWarnEnabled()) {
                                            LOG.warn("Rolling back transaction on error: " + throwable.getMessage(), throwable);
                                        }
                                        return Flux.from(connection.rollbackTransaction())
                                                .hasElements()
                                                .onErrorResume((rollbackError) -> {
                                                    if (rollbackError != throwable && LOG.isWarnEnabled()) {
                                                        LOG.warn("Error occurred during transaction rollback: " + rollbackError.getMessage(), rollbackError);
                                                    }
                                                    return Mono.error(throwable);
                                                }).doFinally((sig) -> status.completed = true);

                                    },
                                    (b) -> doCommit(status));
                        }
                );
            }
        });

    }

    private Publisher<Void> doCommit(DefaultReactiveTransactionStatus status) {
        if (status.isRollbackOnly()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Rolling back transaction on DataSource {}.", dataSourceName);
            }
            return Flux.from(status.getConnection().rollbackTransaction()).doFinally(sig -> status.completed = true);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing transaction for DataSource {}.", dataSourceName);
            }
            return Flux.from(status.getConnection().commitTransaction()).doFinally(sig -> status.completed = true);
        }
    }

    private static <R> Mono<R> toSingleResult(Flux<R> flux) {
        return flux.collectList().flatMap(result -> {
            if (result.isEmpty()) {
                return Mono.empty();
            }
            if (result.size() > 1) {
                return Mono.error(new NonUniqueResultException());
            }
            return Mono.just(result.get(0));
        });
    }

    /**
     * Represents the current reactive transaction status.
     */
    private static final class DefaultReactiveTransactionStatus implements ReactiveTransactionStatus<Connection> {
        private final Connection connection;
        private final boolean isNew;
        private boolean rollbackOnly;
        private boolean completed;

        public DefaultReactiveTransactionStatus(Connection connection, boolean isNew) {
            this.connection = connection;
            this.isNew = isNew;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public boolean isNewTransaction() {
            return isNew;
        }

        @Override
        public void setRollbackOnly() {
            this.rollbackOnly = true;
        }

        @Override
        public boolean isRollbackOnly() {
            return rollbackOnly;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }
    }

    /**
     * reactive operations implementation.
     */
    private final class DefaultR2dbcReactiveRepositoryOperations implements ReactorReactiveRepositoryOperations {

        @Override
        public <T> Mono<Boolean> exists(@NonNull PreparedQuery<T, Boolean> preparedQuery) {
            return Flux.from(withNewOrExistingTransaction(preparedQuery, false, status -> {
                @SuppressWarnings("Convert2MethodRef") Statement statement = prepareStatement(
                        status.getConnection(),
                        (sql) -> status.getConnection().createStatement(sql),
                        preparedQuery,
                        false,
                        true
                );
                return Flux.from(statement.execute())
                        .flatMap((r) ->
                                Flux.from(r.map((row, metadata) -> true))
                        );
            })).collectList().map(results -> !results.isEmpty()).defaultIfEmpty(false);
            // Read full Flux because some drivers doesn't properly handle `cancel`
        }

        @NonNull
        @Override
        public <T, R> Mono<R> findOne(@NonNull PreparedQuery<T, R> preparedQuery) {
            return Flux.from(withNewOrExistingTransaction(preparedQuery, false, status -> {
                @SuppressWarnings("Convert2MethodRef") Statement statement = prepareStatement(
                        status.getConnection(),
                        (sql) -> status.getConnection().createStatement(sql),
                        preparedQuery,
                        false,
                        true
                );
                return Flux.from(statement.execute())
                        .flatMap((r) -> {
                            if (preparedQuery.getResultDataType() == DataType.ENTITY) {
                                Class<R> resultType = preparedQuery.getResultType();
                                RuntimePersistentEntity<R> persistentEntity = getEntity(resultType);
                                SqlResultEntityTypeMapper<Row, R> mapper = new SqlResultEntityTypeMapper<>(
                                        persistentEntity,
                                        columnNameResultSetReader,
                                        preparedQuery.getJoinFetchPaths(),
                                        jsonCodec,
                                        (loadedEntity, o) -> {
                                            if (loadedEntity.hasPostLoadEventListeners()) {
                                                return triggerPostLoad(o, loadedEntity, preparedQuery.getAnnotationMetadata());
                                            } else {
                                                return o;
                                            }
                                        },
                                        conversionService);
                                SqlResultEntityTypeMapper.PushingMapper<Row, R> rowsMapper = mapper.readOneWithJoins();
                                return Flux.from(r.map((row, metadata) -> {
                                    rowsMapper.processRow(row);
                                    return "";
                                })).collectList().flatMap(ignore -> Mono.justOrEmpty(rowsMapper.getResult()));
                            }
                            Class<R> resultType = preparedQuery.getResultType();
                            if (preparedQuery.isDtoProjection()) {
                                return Flux.from(r.map((row, metadata) -> {
                                    TypeMapper<Row, R> introspectedDataMapper = new DTOMapper<>(
                                            getEntity(preparedQuery.getRootEntity()),
                                            columnNameResultSetReader,
                                            jsonCodec,
                                            conversionService);
                                    return introspectedDataMapper.map(row, resultType);
                                }));
                            }
                            return Flux.from(r.map((row, metadata) -> {
                                Object v = columnIndexResultSetReader.readDynamic(row, 0, preparedQuery.getResultDataType());
                                if (v == null) {
                                    return Flux.<R>empty();
                                } else if (resultType.isInstance(v)) {
                                    return Flux.just((R) v);
                                } else {
                                    return Flux.just(columnIndexResultSetReader.convertRequired(v, resultType));
                                }
                            })).flatMap(m -> m);
                        });
            })).as(DefaultR2dbcRepositoryOperations::toSingleResult);
        }

        @NonNull
        @Override
        public <T, R> Flux<R> findAll(@NonNull PreparedQuery<T, R> preparedQuery) {
            return Flux.from(withNewOrExistingTransaction(preparedQuery, false, status -> {
                @SuppressWarnings("Convert2MethodRef") Statement statement = prepareStatement(
                        status.getConnection(),
                        (sql) -> status.getConnection().createStatement(sql),
                        preparedQuery,
                        false,
                        false
                );
                Class<R> resultType = preparedQuery.getResultType();
                boolean dtoProjection = preparedQuery.isDtoProjection();
                boolean isEntity = preparedQuery.getResultDataType() == DataType.ENTITY;
                return Flux.from(statement.execute())
                        .flatMap(r -> {
                            if (isEntity || dtoProjection) {
                                TypeMapper<Row, R> mapper;
                                RuntimePersistentEntity<R> persistentEntity = getEntity(resultType);
                                if (dtoProjection) {
                                    mapper = new SqlDTOMapper<>(
                                            persistentEntity,
                                            columnNameResultSetReader,
                                            jsonCodec,
                                            conversionService
                                    );
                                } else {
                                    Set<JoinPath> joinFetchPaths = preparedQuery.getJoinFetchPaths();
                                    SqlResultEntityTypeMapper<Row, R> entityTypeMapper = new SqlResultEntityTypeMapper<>(
                                            persistentEntity,
                                            columnNameResultSetReader,
                                            joinFetchPaths,
                                            jsonCodec,
                                            (loadedEntity, o) -> {
                                                if (loadedEntity.hasPostLoadEventListeners()) {
                                                    return triggerPostLoad(o, loadedEntity, preparedQuery.getAnnotationMetadata());
                                                } else {
                                                    return o;
                                                }
                                            },
                                            conversionService);
                                    boolean onlySingleEndedJoins = isOnlySingleEndedJoins(getEntity(preparedQuery.getRootEntity()), joinFetchPaths);
                                    // Cannot stream ResultSet for "many" joined query
                                    if (!onlySingleEndedJoins) {
                                        SqlResultEntityTypeMapper.PushingMapper<Row, List<R>> manyReader = entityTypeMapper.readAllWithJoins();
                                        return Flux.from(r.map((row, rowMetadata) -> {
                                            manyReader.processRow(row);
                                            return "";
                                        })).collectList().flatMapIterable(ignore -> manyReader.getResult());
                                    } else {
                                        mapper = entityTypeMapper;
                                    }
                                }
                                return Flux.from(r.map((row, rowMetadata) -> mapper.map(row, resultType)));
                            } else {
                                return Flux.from(r.map((row, rowMetadata) -> {
                                    Object v = columnIndexResultSetReader.readDynamic(row, 0, preparedQuery.getResultDataType());
                                    if (v == null) {
                                        return Mono.<R>empty();
                                    } else if (resultType.isInstance(v)) {
                                        return Mono.just((R) v);
                                    } else {
                                        Object converted = columnIndexResultSetReader.convertRequired(v, resultType);
                                        if (converted != null) {
                                            return Mono.just((R) converted);
                                        } else {
                                            return Mono.<R>empty();
                                        }
                                    }
                                })).flatMap(m -> m);
                            }
                        });
            }));
        }

        @NonNull
        @Override
        public Mono<Number> executeUpdate(@NonNull PreparedQuery<?, Number> preparedQuery) {
            return Flux.from(withNewOrExistingTransaction(preparedQuery, true, status -> {
                @SuppressWarnings("Convert2MethodRef") Statement statement = prepareStatement(
                        status.getConnection(),
                        (sql) -> status.getConnection().createStatement(sql),
                        preparedQuery,
                        true,
                        true
                );
                return Flux.from(statement.execute())
                        .flatMap((result) -> Flux.from(result.getRowsUpdated()).flatMap(rowsUpdated -> {
                            if (QUERY_LOG.isTraceEnabled()) {
                                QUERY_LOG.trace("Update operation updated {} records", rowsUpdated);
                            }
                            if (preparedQuery.isOptimisticLock()) {
                                checkOptimisticLocking(1, rowsUpdated);
                            }
                            Argument<?> argument = preparedQuery.getResultArgument().getFirstTypeVariable().orElse(null);
                            if (argument != null) {
                                if (argument.isVoid() || argument.getType() == Void.class) {
                                    return Mono.<Number>empty();
                                } else if (argument.getType().isInstance(rowsUpdated)) {
                                    return Mono.just(rowsUpdated);
                                } else {
                                    return Mono.just((Number) columnIndexResultSetReader.convertRequired(rowsUpdated, argument));
                                }
                            }
                            return Mono.just(rowsUpdated);
                        }));
            })).as(DefaultR2dbcRepositoryOperations::toSingleResult);
        }

        @NonNull
        @Override
        public Mono<Number> executeDelete(@NonNull PreparedQuery<?, Number> preparedQuery) {
            return executeUpdate(preparedQuery);
        }

        @NonNull
        @Override
        public <T> Mono<Number> delete(@NonNull DeleteOperation<T> operation) {
            SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(operation.getRepositoryType(), DEFAULT_SQL_BUILDER);
            return Flux.<Number>from(withNewOrExistingTransaction(operation, true, status -> {
                StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                R2dbcEntityOperations<T> op = new R2dbcEntityOperations<>(getEntity(operation.getRootEntity()), operation.getEntity(), dbOperation);
                deleteOne(status.getConnection(), op);
                return op.getRowsUpdated();
            })).as(DefaultR2dbcRepositoryOperations::toSingleResult);
        }

        @NonNull
        @Override
        public <T> Flux<T> persistAll(@NonNull InsertBatchOperation<T> operation) {
            return Flux.from(withNewOrExistingTransaction(operation, true, status -> {
                final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
                final Class<?> repositoryType = operation.getRepositoryType();
                SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(repositoryType, DEFAULT_SQL_BUILDER);
                final DBOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                final RuntimePersistentEntity<T> persistentEntity = getEntity(operation.getRootEntity());
                OperationContext operationContext = new OperationContext(annotationMetadata, repositoryType, queryBuilder.dialect());
                if (!isSupportsBatchInsert(persistentEntity, queryBuilder.dialect())) {
                    return concatMono(
                            operation.split().stream()
                                    .map(persistOp -> {
                                        R2dbcEntityOperations<T> op = new R2dbcEntityOperations<>(dbOperation, persistentEntity, persistOp.getEntity(), true);
                                        persistOneSync(status.getConnection(), op, operationContext);
                                        return op.getEntity();
                                    })
                    );
                } else {
                    R2dbcEntitiesOperations<T> op = new R2dbcEntitiesOperations<>(dbOperation, persistentEntity, operation, true);
                    persistInBatch(status.getConnection(), op, operationContext);
                    return op.getEntities();
                }
            }));
        }

        @NonNull
        @Override
        public <T, R> Mono<R> findOptional(@NonNull PreparedQuery<T, R> preparedQuery) {
            return findOne(preparedQuery);
        }

        @NonNull
        @Override
        public <T> Mono<T> persist(@NonNull InsertOperation<T> operation) {
            final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
            SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(operation.getRepositoryType(), DEFAULT_SQL_BUILDER);
            StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
            OperationContext operationContext = new OperationContext(annotationMetadata, operation.getRepositoryType(), queryBuilder.dialect());
            return Flux.from(withNewOrExistingTransaction(operation, true, status -> {
                R2dbcEntityOperations<T> op = new R2dbcEntityOperations<>(dbOperation, getEntity(operation.getRootEntity()), operation.getEntity(), true);
                persistOneSync(status.getConnection(), op, operationContext);
                return op.getEntity();
            })).as(DefaultR2dbcRepositoryOperations::toSingleResult);
        }

        @NonNull
        @Override
        public <T> Mono<T> update(@NonNull UpdateOperation<T> operation) {
            final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
            SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(operation.getRepositoryType(), DEFAULT_SQL_BUILDER);
            StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
            OperationContext operationContext = new OperationContext(annotationMetadata, operation.getRepositoryType(), queryBuilder.dialect());
            return Flux.from(withNewOrExistingTransaction(operation, true, status -> {
                R2dbcEntityOperations<T> op = new R2dbcEntityOperations<>(getEntity(operation.getRootEntity()), operation.getEntity(), dbOperation);
                updateOneSync(status.getConnection(), op, operationContext);
                return op.getEntity();
            })).as(DefaultR2dbcRepositoryOperations::toSingleResult);
        }

        private @NonNull
        TransactionDefinition newTransactionDefinition(AttributeHolder attributeHolder) {
            return attributeHolder.getAttribute(ReactiveTransactionStatus.ATTRIBUTE, TransactionDefinition.class).orElseGet(() -> {
                if (attributeHolder instanceof AnnotationMetadataProvider) {

                    AnnotationValue<TransactionalAdvice> annotation = ((AnnotationMetadataProvider) attributeHolder)
                            .getAnnotationMetadata().getAnnotation(TransactionalAdvice.class);

                    if (annotation != null) {
                        DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
                        attribute.setReadOnly(annotation.isTrue("readOnly"));
                        annotation.intValue("timeout").ifPresent(value -> attribute.setTimeout(Duration.ofSeconds(value)));
                        final Class[] noRollbackFors = annotation.classValues("noRollbackFor");
                        //noinspection unchecked
                        attribute.setNoRollbackFor(noRollbackFors);
                        annotation.enumValue("propagation", TransactionDefinition.Propagation.class)
                                .ifPresent(attribute::setPropagationBehavior);
                        annotation.enumValue("isolation", TransactionDefinition.Isolation.class)
                                .ifPresent(attribute::setIsolationLevel);
                        return attribute;
                    }
                }
                return TransactionDefinition.DEFAULT;
            });
        }

        private <T, R> Publisher<R> withNewOrExistingTransaction(
                @NonNull EntityOperation<T> operation,
                boolean isWrite,
                TransactionalCallback<Connection, R> entityOperation) {
            @SuppressWarnings("unchecked")
            ReactiveTransactionStatus<Connection> connection = operation
                    .getParameterInRole(R2dbcRepository.PARAMETER_TX_STATUS, ReactiveTransactionStatus.class).orElse(null);
            if (connection != null) {
                try {
                    return entityOperation.doInTransaction(connection);
                } catch (Exception e) {
                    return Mono.error(e);
                }
            } else {
                return withNewOrExistingTxAttribute(operation, entityOperation, isWrite);
            }
        }

        private <T, R> Publisher<R> withNewOrExistingTransaction(
                @NonNull PreparedQuery<T, R> operation,
                boolean isWrite,
                TransactionalCallback<Connection, R> entityOperation) {
            @SuppressWarnings("unchecked")
            ReactiveTransactionStatus<Connection> connection = operation
                    .getParameterInRole(R2dbcRepository.PARAMETER_TX_STATUS, ReactiveTransactionStatus.class).orElse(null);
            if (connection != null) {
                try {
                    return entityOperation.doInTransaction(connection);
                } catch (Exception e) {
                    return Mono.error(new TransactionSystemException("Error invoking doInTransaction handler: " + e.getMessage(), e));
                }
            } else {
                return withNewOrExistingTxAttribute(operation, entityOperation, isWrite);
            }
        }

        private <R> Publisher<R> withNewOrExistingTxAttribute(
                @NonNull AttributeHolder attributeHolder,
                TransactionalCallback<Connection, R> entityOperation,
                boolean isWrite) {
            TransactionDefinition definition = newTransactionDefinition(attributeHolder);
            if (isWrite && definition.isReadOnly()) {
                return Mono.error(new TransactionUsageException("Cannot perform write operation with read-only transaction"));
            }
            return withTransaction(definition, entityOperation);
        }

        @NonNull
        @Override
        public <T> Mono<Number> deleteAll(DeleteBatchOperation<T> operation) {
            return Flux.<Number>from(withNewOrExistingTransaction(operation, true, status -> {
                SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(operation.getRepositoryType(), DEFAULT_SQL_BUILDER);
                RuntimePersistentEntity<T> persistentEntity = getEntity(operation.getRootEntity());
                if (isSupportsBatchDelete(persistentEntity, queryBuilder.dialect())) {
                    StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                    R2dbcEntitiesOperations<T> op = new R2dbcEntitiesOperations<>(persistentEntity, operation, dbOperation);
                    deleteInBatch(status.getConnection(), op, dbOperation);
                    return op.getRowsUpdated();
                }
                return sum(
                        operation.split().stream()
                                .map(deleteOp -> {
                                    StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                                    R2dbcEntityOperations<T> op = new R2dbcEntityOperations<>(persistentEntity, deleteOp.getEntity(), dbOperation);
                                    deleteOne(status.getConnection(), op);
                                    return op.getRowsUpdated();
                                })
                );
            })).as(DefaultR2dbcRepositoryOperations::toSingleResult);
        }

        @NonNull
        @Override
        public <T> Flux<T> updateAll(@NonNull UpdateBatchOperation<T> operation) {
            return Flux.from(withNewOrExistingTransaction(operation, true, status -> {
                final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
                final Class<?> repositoryType = operation.getRepositoryType();
                SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(repositoryType, DEFAULT_SQL_BUILDER);
                final RuntimePersistentEntity<T> persistentEntity = getEntity(operation.getRootEntity());
                StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                OperationContext operationContext = new OperationContext(annotationMetadata, operation.getRepositoryType(), queryBuilder.dialect());
                if (!isSupportsBatchUpdate(persistentEntity, queryBuilder.dialect())) {
                    return concatMono(
                            operation.split().stream()
                                    .map(updateOp -> {
                                        R2dbcEntityOperations<T> op = new R2dbcEntityOperations<>(persistentEntity, updateOp.getEntity(), dbOperation);
                                        updateOneSync(status.getConnection(), op, operationContext);
                                        return op.getEntity();
                                    })
                    );
                }
                R2dbcEntitiesOperations<T> op = new R2dbcEntitiesOperations<>(persistentEntity, operation, dbOperation);
                updateInBatch(status.getConnection(), op, operationContext);
                return op.getEntities();

            }));
        }

        @NonNull
        @Override
        public <T> Mono<T> findOptional(@NonNull Class<T> type, @NonNull Serializable id) {
            throw new UnsupportedOperationException("The findOptional method by ID is not supported. Execute the SQL query directly");
        }

        @NonNull
        @Override
        public <R> Mono<Page<R>> findPage(@NonNull PagedQuery<R> pagedQuery) {
            throw new UnsupportedOperationException("The findPage method is not supported. Execute the SQL query directly");
        }

        @NonNull
        @Override
        public <T> Mono<T> findOne(@NonNull Class<T> type, @NonNull Serializable id) {
            throw new UnsupportedOperationException("The findOne method by ID is not supported. Execute the SQL query directly");
        }

        @NonNull
        @Override
        public <T> Mono<Long> count(PagedQuery<T> pagedQuery) {
            throw new UnsupportedOperationException("The count method without an explicit query is not supported. Use findAll(PreparedQuery) instead");
        }

        @NonNull
        @Override
        public <T> Flux<T> findAll(PagedQuery<T> pagedQuery) {
            throw new UnsupportedOperationException("The findAll method without an explicit query is not supported. Use findAll(PreparedQuery) instead");
        }

    }

    private final class R2dbcEntityOperations<T> extends EntityOperations<T> {

        private final DBOperation dbOperation;
        private final boolean insert;
        private final boolean hasGeneratedId;
        private Mono<Data> data;

        protected R2dbcEntityOperations(RuntimePersistentEntity<T> persistentEntity, T entity, DBOperation dbOperation) {
            this(dbOperation, persistentEntity, entity, false);
        }

        protected R2dbcEntityOperations(DBOperation dbOperation, RuntimePersistentEntity<T> persistentEntity, T entity, boolean insert) {
            super(persistentEntity);
            this.dbOperation = dbOperation;
            this.insert = insert;
            this.hasGeneratedId = insert && persistentEntity.getIdentity() != null && persistentEntity.getIdentity().isGenerated();
            Data data = new Data();
            data.entity = entity;
            this.data = Mono.just(data);
        }

        @Override
        public DBOperation getDbOperation() {
            return dbOperation;
        }

        @Override
        protected String debug() {
            return dbOperation.getQuery();
        }

        @Override
        protected void cascadePre(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            doCascade(false, cascadeType, connection, operationContext);
        }

        @Override
        protected void cascadePost(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            doCascade(true, cascadeType, connection, operationContext);
        }

        private void doCascade(boolean isPost, Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {

            this.data = data.flatMap(d -> {
                if (d.vetoed) {
                    return Mono.just(d);
                }
                Mono<T> entity = cascadeEntity(d.entity, persistentEntity, isPost, cascadeType, connection, operationContext);
                return entity.map(e -> {
                    d.entity = e;
                    return d;
                });
            });
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
            data = data.map(d -> {
                if (d.vetoed) {
                    return d;
                }
                d.previousValues = dbOperation.collectAutoPopulatedPreviousValues(persistentEntity, d.entity);
                return d;
            });
        }

        private Statement prepare(Connection connection) throws RuntimeException {
            if (StoredSqlOperation.class.isInstance(dbOperation)) {
                data = data.map(d -> {
                    if (d.vetoed) {
                        return d;
                    }
                    ((StoredSqlOperation) dbOperation).checkForParameterToBeExpanded(persistentEntity, d.entity);
                    return d;
                });
            }
            Statement statement = connection.createStatement(dbOperation.getQuery());
            if (hasGeneratedId) {
                return statement.returnGeneratedValues(persistentEntity.getIdentity().getPersistedName());
            }
            return statement;
        }

        private void setParameters(OpContext<Connection, Statement> context, Connection connection, Statement stmt, DBOperation sqlOperation) {
            data = data.map(d -> {
                if (d.vetoed) {
                    return d;
                }
                sqlOperation.setParameters(context, connection, stmt, persistentEntity, d.entity, d.previousValues);
                return d;
            });
        }

        @Override
        protected void executeUpdate(OpContext<Connection, Statement> context, Connection connection) throws RuntimeException {
            Statement statement = prepare(connection);
            setParameters(context, connection, statement, dbOperation);
            if (hasGeneratedId) {
                data = data.flatMap(d -> {
                    if (d.vetoed) {
                        return Mono.just(d);
                    }
                    RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                    return Flux.from(statement.execute()).flatMap(result ->
                                    Flux.from(result.map((row, rowMetadata) ->
                                            columnIndexResultSetReader.readDynamic(row, 0, identity.getDataType()))))
                            .as(DefaultR2dbcRepositoryOperations::toSingleResult).map(id -> {
                                BeanProperty<T, Object> property = (BeanProperty<T, Object>) identity.getProperty();
                                d.entity = updateEntityId(property, d.entity, id);
                                return d;
                            });
                });
            } else {
                data = data.flatMap(d -> {
                    if (d.vetoed) {
                        return Mono.just(d);
                    }
                    return Flux.from(statement.execute()).flatMap(r -> Flux.from(r.getRowsUpdated()))
                            // Remove in the future: unneeded call "getRowsUpdated" is required for some drivers
                            .as(DefaultR2dbcRepositoryOperations::toSingleResult)
                            .thenReturn(d);
                });
            }
        }

        @Override
        protected void executeUpdate(OpContext<Connection, Statement> context, Connection connection, DBOperation2<Integer, Integer, RuntimeException> fn) throws RuntimeException {
            Statement statement = prepare(connection);
            setParameters(context, connection, statement, dbOperation);
            data = data.flatMap(d -> Flux.from(statement.execute()).flatMap(result -> Flux.from(result.getRowsUpdated()))
                    .as(DefaultR2dbcRepositoryOperations::toSingleResult)
                    .map(rowsUpdated -> {
                        if (d.vetoed) {
                            return d;
                        }
                        d.rowsUpdated = rowsUpdated;
                        fn.process(1, rowsUpdated);
                        return d;
                    }));
        }

        @Override
        protected boolean triggerPre(Function<EntityEventContext<Object>, Boolean> fn) {
            data = data.map(d -> {
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
            data = data.map(d -> {
                if (d.vetoed) {
                    return d;
                }
                final DefaultEntityEventContext<T> event = new DefaultEntityEventContext<>(persistentEntity, d.entity);
                fn.accept((EntityEventContext<Object>) event);
                return d;
            });
        }

        @Override
        protected void veto(Predicate<T> predicate) {
            data = data.map(d -> {
                if (d.vetoed) {
                    return d;
                }
                d.vetoed = predicate.test(d.entity);
                return d;
            });
        }

        private boolean notVetoed(Data data) {
            return !data.vetoed;
        }

        Mono<T> getEntity() {
            return data.filter(this::notVetoed).map(d -> d.entity);
        }

        Mono<Integer> getRowsUpdated() {
            return data.filter(this::notVetoed).map(d -> d.rowsUpdated).switchIfEmpty(Mono.just(0));
        }

        class Data {
            T entity;
            Map<QueryParameterBinding, Object> previousValues;
            int rowsUpdated;
            boolean vetoed = false;
        }
    }

    private final class R2dbcEntitiesOperations<T> extends EntitiesOperations<T> {

        private final DBOperation dbOperation;
        private final boolean insert;
        private final boolean hasGeneratedId;
        private Flux<Data> entities;
        private Mono<Integer> rowsUpdated;

        private R2dbcEntitiesOperations(RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, DBOperation dbOperation) {
            this(dbOperation, persistentEntity, entities, false);
        }

        private R2dbcEntitiesOperations(DBOperation dbOperation, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, boolean insert) {
            super(persistentEntity);
            this.dbOperation = dbOperation;
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
        public DBOperation getDbOperation() {
            return dbOperation;
        }

        @Override
        protected String debug() {
            return dbOperation.getQuery();
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
            entities = entities.map(d -> {
                if (d.vetoed) {
                    return d;
                }
                d.previousValues = dbOperation.collectAutoPopulatedPreviousValues(persistentEntity, d.entity);
                return d;
            });
        }

        private Statement prepare(Connection connection) throws RuntimeException {
            Statement statement = connection.createStatement(dbOperation.getQuery());
            if (hasGeneratedId) {
                return statement.returnGeneratedValues(persistentEntity.getIdentity().getPersistedName());
            }
            return statement;
        }

        @Override
        protected void cascadePre(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            doCascade(false, cascadeType, connection, operationContext);
        }

        @Override
        protected void cascadePost(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            doCascade(true, cascadeType, connection, operationContext);
        }

        private void doCascade(boolean isPost, Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {

            this.entities = entities.flatMap(d -> {
                if (d.vetoed) {
                    return Mono.just(d);
                }
                Mono<T> entity = cascadeEntity(d.entity, persistentEntity, isPost, cascadeType, connection, operationContext);
                return entity.map(e -> {
                    d.entity = e;
                    return d;
                });
            });
        }

        @Override
        protected void veto(Predicate<T> predicate) {
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

        private void setParameters(OpContext<Connection, Statement> context, Connection connection, Statement stmt, DBOperation dbOperation) {
            entities = entities.map(d -> {
                if (d.vetoed) {
                    return d;
                }
                dbOperation.setParameters(context, connection, stmt, persistentEntity, d.entity, d.previousValues);
                stmt.add();
                return d;
            });
        }

        @Override
        protected void executeUpdate(OpContext<Connection, Statement> context, Connection connection) throws RuntimeException {
            Statement statement = prepare(connection);
            setParameters(context, connection, statement, dbOperation);
            if (hasGeneratedId) {
                entities = entities.collectList()
                        .flatMapMany(e -> {
                            List<Data> notVetoedEntities = e.stream().filter(this::notVetoed).collect(Collectors.toList());
                            if (notVetoedEntities.isEmpty()) {
                                return Flux.fromIterable(notVetoedEntities);
                            }
                            Mono<List<Object>> ids = Flux.from(statement.execute())
                                    .flatMap(result ->
                                            Flux.from(result.map((row, rowMetadata)
                                                    -> columnIndexResultSetReader.readDynamic(row, 0, persistentEntity.getIdentity().getDataType())))
                                    ).collectList();

                            return ids.flatMapMany(idList -> {
                                Iterator<Object> iterator = idList.iterator();
                                ListIterator<Data> resultIterator = notVetoedEntities.listIterator();
                                RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                                while (resultIterator.hasNext()) {
                                    Data d = resultIterator.next();
                                    if (!iterator.hasNext()) {
                                        throw new DataAccessException("Failed to generate ID for entity: " + d.entity);
                                    } else {
                                        Object id = iterator.next();
                                        d.entity = updateEntityId((BeanProperty<T, Object>) identity.getProperty(), d.entity, id);
                                    }
                                }
                                return Flux.fromIterable(e);
                            });
                        });
            } else {
                entities = entities.collectList()
                        .flatMapMany(e -> {
                            List<Data> notVetoedEntities = e.stream().filter(this::notVetoed).collect(Collectors.toList());
                            if (notVetoedEntities.isEmpty()) {
                                return Flux.fromIterable(e);
                            }
                            // Remove in the future: unneeded call "getRowsUpdated" is required for some drivers
                            return Flux.from(statement.execute()).flatMap(result -> Flux.from(result.getRowsUpdated())).thenMany(Flux.fromIterable(e));
                        });
            }
        }

        @Override
        protected void executeUpdate(OpContext<Connection, Statement> context, Connection connection, DBOperation2<Integer, Integer, RuntimeException> fn) throws RuntimeException {
            Statement statement = prepare(connection);
            setParameters(context, connection, statement, dbOperation);
            Mono<Tuple2<List<Data>, Integer>> entitiesWithRowsUpdated = entities.collectList()
                    .flatMap(e -> {
                        List<Data> notVetoedEntities = e.stream().filter(this::notVetoed).collect(Collectors.toList());
                        if (notVetoedEntities.isEmpty()) {
                            return Mono.just(Tuples.of(e, 0));
                        }
                        return Flux.from(statement.execute()).flatMap(result -> Flux.from(result.getRowsUpdated())).reduce(0, Integer::sum)
                                .map(rowsUpdated -> {
                                    fn.process(notVetoedEntities.size(), rowsUpdated);
                                    return Tuples.of(e, rowsUpdated);
                                });
                    }).cache();
            entities = entitiesWithRowsUpdated.flatMapMany(t -> Flux.fromIterable(t.getT1()));
            rowsUpdated = entitiesWithRowsUpdated.map(Tuple2::getT2);
        }

        private boolean notVetoed(Data data) {
            return !data.vetoed;
        }

        protected Flux<T> getEntities() {
            return entities.map(d -> d.entity);
        }

        protected Mono<Integer> getRowsUpdated() {
            // We need to trigger entities to execute post actions when getting just rows
            return rowsUpdated.flatMap(rows -> entities.then(Mono.just(rows)));
        }

        class Data {
            T entity;
            Map<QueryParameterBinding, Object> previousValues;
            boolean vetoed = false;
        }
    }

    private static final class RuntimePersistentPropertyR2dbcCC extends R2dbcConversionContextImpl implements RuntimePersistentPropertyConversionContext {

        private final RuntimePersistentProperty<?> property;

        public RuntimePersistentPropertyR2dbcCC(Connection connection, RuntimePersistentProperty<?> property) {
            super(ConversionContext.of(property.getArgument()), connection);
            this.property = property;
        }

        @Override
        public RuntimePersistentProperty<?> getRuntimePersistentProperty() {
            return property;
        }
    }

    private static final class ArgumentR2dbcCC extends R2dbcConversionContextImpl implements ArgumentConversionContext<Object> {

        private final Argument argument;

        public ArgumentR2dbcCC(Connection connection, Argument argument) {
            super(ConversionContext.of(argument), connection);
            this.argument = argument;
        }

        @Override
        public Argument<Object> getArgument() {
            return argument;
        }
    }

    private static class R2dbcConversionContextImpl extends AbstractConversionContext
            implements R2dbcConversionContext {

        private final Connection connection;

        public R2dbcConversionContextImpl(Connection connection) {
            this(ConversionContext.DEFAULT, connection);
        }

        public R2dbcConversionContextImpl(ConversionContext conversionContext, Connection connection) {
            super(conversionContext);
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

    }
}
