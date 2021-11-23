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
package io.micronaut.data.jdbc.operations;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.jdbc.convert.JdbcConversionContext;
import io.micronaut.data.jdbc.mapper.ColumnIndexResultSetReader;
import io.micronaut.data.jdbc.mapper.ColumnNameResultSetReader;
import io.micronaut.data.jdbc.mapper.JdbcQueryStatement;
import io.micronaut.data.jdbc.mapper.SqlResultConsumer;
import io.micronaut.data.jdbc.runtime.ConnectionCallback;
import io.micronaut.data.jdbc.runtime.PreparedStatementCallback;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.query.JoinPath;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.query.builder.sql.SqlQueryBuilder;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.DeleteBatchOperation;
import io.micronaut.data.model.runtime.DeleteOperation;
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
import io.micronaut.data.operations.async.AsyncCapableRepository;
import io.micronaut.data.operations.reactive.ReactiveCapableRepository;
import io.micronaut.data.operations.reactive.ReactiveRepositoryOperations;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.convert.RuntimePersistentPropertyConversionContext;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;
import io.micronaut.data.runtime.mapper.DTOMapper;
import io.micronaut.data.runtime.mapper.ResultConsumer;
import io.micronaut.data.runtime.mapper.ResultReader;
import io.micronaut.data.runtime.mapper.TypeMapper;
import io.micronaut.data.runtime.mapper.sql.SqlDTOMapper;
import io.micronaut.data.runtime.mapper.sql.SqlResultEntityTypeMapper;
import io.micronaut.data.runtime.mapper.sql.SqlTypeMapper;
import io.micronaut.data.runtime.operations.ExecutorAsyncOperations;
import io.micronaut.data.runtime.operations.ExecutorReactiveOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSqlRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.DBOperation;
import io.micronaut.data.runtime.operations.internal.OpContext;
import io.micronaut.data.runtime.operations.internal.StoredQuerySqlOperation;
import io.micronaut.data.runtime.operations.internal.StoredSqlOperation;
import io.micronaut.data.runtime.support.AbstractConversionContext;
import io.micronaut.http.codec.MediaTypeCodec;
import io.micronaut.transaction.TransactionOperations;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link JdbcRepositoryOperations}.
 *
 * @author graemerocher
 * @author Denis Stepanov
 * @since 1.0.0
 */
@EachBean(DataSource.class)
@Internal
public final class DefaultJdbcRepositoryOperations extends AbstractSqlRepositoryOperations<Connection, ResultSet, PreparedStatement, SQLException> implements
        JdbcRepositoryOperations,
        AsyncCapableRepository,
        ReactiveCapableRepository,
        AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJdbcRepositoryOperations.class);
    private final TransactionOperations<Connection> transactionOperations;
    private final DataSource dataSource;
    private ExecutorAsyncOperations asyncOperations;
    private ExecutorService executorService;

    /**
     * Default constructor.
     *
     * @param dataSourceName             The data source name
     * @param dataSource                 The datasource
     * @param transactionOperations      The JDBC operations for the data source
     * @param executorService            The executor service
     * @param beanContext                The bean context
     * @param codecs                     The codecs
     * @param dateTimeProvider           The dateTimeProvider
     * @param entityRegistry             The entity registry
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     */
    @Internal
    protected DefaultJdbcRepositoryOperations(@Parameter String dataSourceName,
                                              DataSource dataSource,
                                              @Parameter TransactionOperations<Connection> transactionOperations,
                                              @Named("io") @Nullable ExecutorService executorService,
                                              BeanContext beanContext,
                                              List<MediaTypeCodec> codecs,
                                              @NonNull DateTimeProvider dateTimeProvider,
                                              RuntimeEntityRegistry entityRegistry,
                                              DataConversionService<?> conversionService,
                                              AttributeConverterRegistry attributeConverterRegistry) {
        super(
                dataSourceName,
                new ColumnNameResultSetReader(conversionService),
                new ColumnIndexResultSetReader(conversionService),
                new JdbcQueryStatement(conversionService),
                codecs,
                dateTimeProvider,
                entityRegistry,
                beanContext,
                conversionService, attributeConverterRegistry);
        ArgumentUtils.requireNonNull("dataSource", dataSource);
        ArgumentUtils.requireNonNull("transactionOperations", transactionOperations);
        this.dataSource = dataSource;
        this.transactionOperations = transactionOperations;
        this.executorService = executorService;
    }

    @NonNull
    private ExecutorService newLocalThreadPool() {
        this.executorService = Executors.newCachedThreadPool();
        return executorService;
    }

    private <T> T cascadeEntity(T entity,
                                RuntimePersistentEntity<T> persistentEntity,
                                boolean isPost,
                                Relation.Cascade cascadeType,
                                Connection connection,
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
                    JdbcEntityOperations<Object> op = new JdbcEntityOperations<>(childPersistentEntity, child, true);
                    DBOperation childSqlPersistOperation = resolveEntityInsert(operationContext.annotationMetadata, operationContext.repositoryType, child.getClass(), childPersistentEntity);
                    persistOne(connection, childSqlPersistOperation, op, operationContext);
                    entity = afterCascadedOne(entity, cascadeOp.ctx.associations, child, op.entity);
                    child = op.entity;
                } else if (hasId && (cascadeType == Relation.Cascade.UPDATE)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Cascading MERGE for '{}' ({}) association: '{}'", persistentEntity.getName(),
                                persistentEntity.getIdentity().getProperty().get(entity), cascadeOp.ctx.associations);
                    }
                    JdbcEntityOperations<Object> op = new JdbcEntityOperations<>(childPersistentEntity, child);
                    DBOperation childSqlUpdateOperation = resolveEntityUpdate(operationContext.annotationMetadata, operationContext.repositoryType, child.getClass(), childPersistentEntity);
                    updateOne(connection, childSqlUpdateOperation, op, operationContext);
                    entity = afterCascadedOne(entity, cascadeOp.ctx.associations, child, op.entity);
                    child = op.entity;
                }
                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                if (!hasId
                        && (cascadeType == Relation.Cascade.PERSIST || cascadeType == Relation.Cascade.UPDATE)
                        && SqlQueryBuilder.isForeignKeyWithJoinTable(association)) {
                    persistJoinTableAssociation(connection, operationContext.repositoryType, operationContext.dialect, association, entity,
                            new JdbcEntityOperations<>(childPersistentEntity, child));
                }
                operationContext.persisted.add(child);
            } else if (cascadeOp instanceof CascadeManyOp) {
                CascadeManyOp cascadeManyOp = (CascadeManyOp) cascadeOp;
                RuntimePersistentEntity<Object> childPersistentEntity = cascadeManyOp.childPersistentEntity;

                List<Object> entities;
                if (cascadeType == Relation.Cascade.UPDATE) {
                    entities = CollectionUtils.iterableToList(cascadeManyOp.children);
                    DBOperation childSqlUpdateOperation = null;
                    DBOperation childSqlInsertOperation = null;
                    for (ListIterator<Object> iterator = entities.listIterator(); iterator.hasNext(); ) {
                        Object child = iterator.next();
                        if (operationContext.persisted.contains(child)) {
                            continue;
                        }

                        JdbcEntityOperations<Object> op = new JdbcEntityOperations<>(childPersistentEntity, child, true);

                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        if (identity.getProperty().get(child) == null) {
                            if (childSqlInsertOperation == null) {
                                childSqlInsertOperation = resolveEntityInsert(operationContext.annotationMetadata, operationContext.repositoryType, childPersistentEntity.getIntrospection().getBeanType(), childPersistentEntity);
                            }
                            persistOne(connection, childSqlInsertOperation, op, operationContext);
                        } else {
                            if (childSqlUpdateOperation == null) {
                                childSqlUpdateOperation = resolveEntityUpdate(operationContext.annotationMetadata, operationContext.repositoryType, childPersistentEntity.getIntrospection().getBeanType(), childPersistentEntity);
                            }
                            updateOne(connection, childSqlUpdateOperation, op, operationContext);
                        }

                        iterator.set(op.entity);
                    }
                } else if (cascadeType == Relation.Cascade.PERSIST) {

                    DBOperation childSqlPersistOperation = resolveEntityInsert(
                            operationContext.annotationMetadata,
                            operationContext.repositoryType,
                            childPersistentEntity.getIntrospection().getBeanType(),
                            childPersistentEntity
                    );

                    if (isSupportsBatchInsert(childPersistentEntity, operationContext.dialect)) {
                        JdbcEntitiesOperations<Object> op = new JdbcEntitiesOperations<>(childPersistentEntity, cascadeManyOp.children, true);
                        op.veto(operationContext.persisted::contains);
                        RuntimePersistentProperty<Object> identity = childPersistentEntity.getIdentity();
                        op.veto(e -> identity.getProperty().get(e) != null);

                        persistInBatch(connection, childSqlPersistOperation, op, operationContext);

                        entities = op.getEntities();
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

                            JdbcEntityOperations<Object> op = new JdbcEntityOperations<>(childPersistentEntity, child, true);

                            persistOne(connection, childSqlPersistOperation, op, operationContext);

                            iterator.set(op.entity);
                        }
                    }
                } else {
                    continue;
                }

                entity = afterCascadedMany(entity, cascadeOp.ctx.associations, cascadeManyOp.children, entities);

                RuntimeAssociation<Object> association = (RuntimeAssociation) cascadeOp.ctx.getAssociation();
                if (SqlQueryBuilder.isForeignKeyWithJoinTable(association) && !entities.isEmpty()) {
                    if (operationContext.dialect.allowBatch()) {
                        JdbcEntitiesOperations<Object> assocOp = new JdbcEntitiesOperations<>(childPersistentEntity, entities);
                        assocOp.veto(operationContext.persisted::contains);
                        persistJoinTableAssociation(connection, operationContext.repositoryType, operationContext.dialect, association, cascadeOp.ctx.parent, assocOp);
                    } else {
                        for (Object e : cascadeManyOp.children) {
                            if (operationContext.persisted.contains(e)) {
                                continue;
                            }
                            persistJoinTableAssociation(connection, operationContext.repositoryType, operationContext.dialect, association, cascadeOp.ctx.parent,
                                    new JdbcEntityOperations<>(childPersistentEntity, e));
                        }
                    }
                }

                operationContext.persisted.addAll(entities);
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
            return new RuntimePersistentPropertyJdbcCC(connection, property);
        }
        if (argument != null) {
            return new ArgumentJdbcCC(connection, argument);
        }
        return new JdbcConversionContextImpl(connection);
    }

    @NonNull
    @Override
    public ExecutorAsyncOperations async() {
        ExecutorAsyncOperations asyncOperations = this.asyncOperations;
        if (asyncOperations == null) {
            synchronized (this) { // double check
                asyncOperations = this.asyncOperations;
                if (asyncOperations == null) {
                    asyncOperations = new ExecutorAsyncOperations(
                            this,
                            executorService != null ? executorService : newLocalThreadPool()
                    );
                    this.asyncOperations = asyncOperations;
                }
            }
        }
        return asyncOperations;
    }

    @NonNull
    @Override
    public ReactiveRepositoryOperations reactive() {
        return new ExecutorReactiveOperations(async(), conversionService);
    }

    @Nullable
    @Override
    public <T, R> R findOne(@NonNull PreparedQuery<T, R> preparedQuery) {
        return transactionOperations.executeRead(status -> {
            Connection connection = status.getConnection();
            RuntimePersistentEntity<T> persistentEntity = getEntity(preparedQuery.getRootEntity());
            try (PreparedStatement ps = prepareStatement(connection, connection::prepareStatement, preparedQuery, false, true)) {
                try (ResultSet rs = ps.executeQuery()) {
                    Class<R> resultType = preparedQuery.getResultType();
                    if (preparedQuery.getResultDataType() == DataType.ENTITY) {
                        RuntimePersistentEntity<R> resultPersistentEntity = getEntity(resultType);

                        final Set<JoinPath> joinFetchPaths = preparedQuery.getJoinFetchPaths();
                        SqlResultEntityTypeMapper<ResultSet, R> mapper = new SqlResultEntityTypeMapper<>(
                                resultPersistentEntity,
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
                        SqlResultEntityTypeMapper.PushingMapper<ResultSet, R> oneMapper = mapper.readOneWithJoins();
                        if (rs.next()) {
                            oneMapper.processRow(rs);
                        }
                        while (!joinFetchPaths.isEmpty() && rs.next()) {
                            oneMapper.processRow(rs);
                        }
                        R result = oneMapper.getResult();
                        if (preparedQuery.hasResultConsumer()) {
                            preparedQuery.getParameterInRole(SqlResultConsumer.ROLE, SqlResultConsumer.class)
                                    .ifPresent(consumer -> consumer.accept(result, newMappingContext(rs)));
                        }
                        return result;
                    } else if (rs.next()) {
                        if (preparedQuery.isDtoProjection()) {
                            TypeMapper<ResultSet, R> introspectedDataMapper = new DTOMapper<>(
                                    persistentEntity,
                                    columnNameResultSetReader,
                                    jsonCodec,
                                    conversionService);
                            return introspectedDataMapper.map(rs, resultType);
                        } else {
                            Object v = columnIndexResultSetReader.readDynamic(rs, 1, preparedQuery.getResultDataType());
                            if (v == null) {
                                return null;
                            } else if (resultType.isInstance(v)) {
                                return (R) v;
                            } else {
                                return columnIndexResultSetReader.convertRequired(v, resultType);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                throw new DataAccessException("Error executing SQL Query: " + e.getMessage(), e);
            }
            return null;
        });
    }

    @Override
    public <T> boolean exists(@NonNull PreparedQuery<T, Boolean> preparedQuery) {
        return transactionOperations.executeRead(status -> {
            try {
                Connection connection = status.getConnection();
                try (PreparedStatement ps = prepareStatement(connection, connection::prepareStatement, preparedQuery, false, true)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next();
                    }
                }
            } catch (SQLException e) {
                throw new DataAccessException("Error executing SQL query: " + e.getMessage(), e);
            }
        });
    }

    @NonNull
    @Override
    public <T, R> Stream<R> findStream(@NonNull PreparedQuery<T, R> preparedQuery) {
        return findStream(preparedQuery, transactionOperations.getConnection());
    }

    private <T, R> Stream<R> findStream(@NonNull PreparedQuery<T, R> preparedQuery, Connection connection) {
        Class<R> resultType = preparedQuery.getResultType();
        AtomicBoolean finished = new AtomicBoolean();

        PreparedStatement ps;
        try {
            ps = prepareStatement(connection, connection::prepareStatement, preparedQuery, false, false);
        } catch (Exception e) {
            throw new DataAccessException("SQL Error preparing Query: " + e.getMessage(), e);
        }

        ResultSet openedRs = null;
        ResultSet rs;
        try {
            openedRs = ps.executeQuery();
            rs = openedRs;

            boolean dtoProjection = preparedQuery.isDtoProjection();
            boolean isEntity = preparedQuery.getResultDataType() == DataType.ENTITY;
            Spliterator<R> spliterator;

            if (isEntity || dtoProjection) {
                SqlResultConsumer sqlMappingConsumer = preparedQuery.hasResultConsumer() ? preparedQuery.getParameterInRole(SqlResultConsumer.ROLE, SqlResultConsumer.class).orElse(null) : null;
                SqlTypeMapper<ResultSet, R> mapper;
                final RuntimePersistentEntity<R> persistentEntity = getEntity(resultType);
                if (dtoProjection) {
                    mapper = new SqlDTOMapper<>(
                            persistentEntity,
                            columnNameResultSetReader,
                            jsonCodec,
                            conversionService
                    );
                } else {
                    Set<JoinPath> joinFetchPaths = preparedQuery.getJoinFetchPaths();
                    SqlResultEntityTypeMapper<ResultSet, R> entityTypeMapper = new SqlResultEntityTypeMapper<>(
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
                        try {
                            SqlResultEntityTypeMapper.PushingMapper<ResultSet, List<R>> manyMapper = entityTypeMapper.readAllWithJoins();
                            while (rs.next()) {
                                manyMapper.processRow(rs);
                            }
                            return manyMapper.getResult().stream();
                        } finally {
                            closeResultSet(ps, rs, finished);
                        }
                    } else {
                        mapper = entityTypeMapper;
                    }
                }
                spliterator = new Spliterators.AbstractSpliterator<R>(Long.MAX_VALUE,
                        Spliterator.ORDERED | Spliterator.IMMUTABLE) {
                    @Override
                    public boolean tryAdvance(Consumer<? super R> action) {
                        if (finished.get()) {
                            return false;
                        }
                        boolean hasNext = mapper.hasNext(rs);
                        if (hasNext) {
                            R o = mapper.map(rs, resultType);
                            if (sqlMappingConsumer != null) {
                                sqlMappingConsumer.accept(rs, o);
                            }
                            action.accept(o);
                        } else {
                            closeResultSet(ps, rs, finished);
                        }
                        return hasNext;
                    }
                };
            } else {
                spliterator = new Spliterators.AbstractSpliterator<R>(Long.MAX_VALUE,
                        Spliterator.ORDERED | Spliterator.IMMUTABLE) {
                    @Override
                    public boolean tryAdvance(Consumer<? super R> action) {
                        if (finished.get()) {
                            return false;
                        }
                        try {
                            boolean hasNext = rs.next();
                            if (hasNext) {
                                Object v = columnIndexResultSetReader
                                        .readDynamic(rs, 1, preparedQuery.getResultDataType());
                                if (resultType.isInstance(v)) {
                                    //noinspection unchecked
                                    action.accept((R) v);
                                } else if (v != null) {
                                    Object r = columnIndexResultSetReader.convertRequired(v, resultType);
                                    if (r != null) {
                                        action.accept((R) r);
                                    }
                                }
                            } else {
                                closeResultSet(ps, rs, finished);
                            }
                            return hasNext;
                        } catch (SQLException e) {
                            throw new DataAccessException("Error retrieving next JDBC result: " + e.getMessage(), e);
                        }
                    }
                };
            }

            return StreamSupport.stream(spliterator, false).onClose(() -> {
                closeResultSet(ps, rs, finished);
            });
        } catch (Exception e) {
            closeResultSet(ps, openedRs, finished);
            throw new DataAccessException("SQL Error executing Query: " + e.getMessage(), e);
        }
    }

    private void closeResultSet(PreparedStatement ps, ResultSet rs, AtomicBoolean finished) {
        if (finished.compareAndSet(false, true)) {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                throw new DataAccessException("Error closing JDBC result stream: " + e.getMessage(), e);
            }
        }
    }

    @NonNull
    @Override
    public <T, R> Iterable<R> findAll(@NonNull PreparedQuery<T, R> preparedQuery) {
        return transactionOperations.executeRead(status -> {
            Connection connection = status.getConnection();
            return findStream(preparedQuery, connection).collect(Collectors.toList());
        });
    }

    @NonNull
    @Override
    public Optional<Number> executeUpdate(@NonNull PreparedQuery<?, Number> preparedQuery) {
        return transactionOperations.executeWrite(status -> {
            try {
                Connection connection = status.getConnection();
                try (PreparedStatement ps = prepareStatement(connection, connection::prepareStatement, preparedQuery, true, false)) {
                    int result = ps.executeUpdate();
                    if (QUERY_LOG.isTraceEnabled()) {
                        QUERY_LOG.trace("Update operation updated {} records", result);
                    }
                    if (preparedQuery.isOptimisticLock()) {
                        checkOptimisticLocking(1, result);
                    }
                    return Optional.of(result);
                }
            } catch (SQLException e) {
                throw new DataAccessException("Error executing SQL UPDATE: " + e.getMessage(), e);
            }
        });
    }

    private Integer sum(Stream<Integer> stream) {
        return stream.mapToInt(i -> i).sum();
    }

    @Override
    public <T> Optional<Number> deleteAll(@NonNull DeleteBatchOperation<T> operation) {
        return Optional.ofNullable(transactionOperations.executeWrite(status -> {
            SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(operation.getRepositoryType(), DEFAULT_SQL_BUILDER);
            Dialect dialect = queryBuilder.dialect();
            RuntimePersistentEntity<T> persistentEntity = getEntity(operation.getRootEntity());
            if (isSupportsBatchDelete(persistentEntity, dialect)) {
                JdbcEntitiesOperations<T> op = new JdbcEntitiesOperations<>(getEntity(operation.getRootEntity()), operation);
                StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                deleteInBatch(status.getConnection(), op, dbOperation);
                return op.rowsUpdated;
            }
            return sum(
                    operation.split().stream()
                            .map(deleteOp -> {
                                JdbcEntityOperations<T> op = new JdbcEntityOperations<>(getEntity(deleteOp.getRootEntity()), deleteOp.getEntity());
                                StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
                                deleteOne(status.getConnection(), op, dbOperation);
                                return op.rowsUpdated;
                            })
            );
        }));
    }

    @Override
    public <T> int delete(@NonNull DeleteOperation<T> operation) {
        SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(operation.getRepositoryType(), DEFAULT_SQL_BUILDER);
        return transactionOperations.executeWrite(status -> {
            JdbcEntityOperations<T> op = new JdbcEntityOperations<>(getEntity(operation.getRootEntity()), operation.getEntity());
            StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
            deleteOne(status.getConnection(), op, dbOperation);
            return op;
        }).rowsUpdated;
    }

    @NonNull
    @Override
    public <T> T update(@NonNull UpdateOperation<T> operation) {
        final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
        final Class<?> repositoryType = operation.getRepositoryType();
        SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(repositoryType, DEFAULT_SQL_BUILDER);
        StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
        return transactionOperations.executeWrite(status -> {
            JdbcEntityOperations<T> op = new JdbcEntityOperations<>(getEntity(operation.getRootEntity()), operation.getEntity());
            updateOne(status.getConnection(), dbOperation, op, new OperationContext(annotationMetadata, repositoryType, queryBuilder.dialect()));
            return op;
        }).entity;
    }

    @NonNull
    @Override
    public <T> Iterable<T> updateAll(@NonNull UpdateBatchOperation<T> operation) {
        return transactionOperations.executeWrite(status -> {
            final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
            final Class<?> repositoryType = operation.getRepositoryType();
            SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(repositoryType, DEFAULT_SQL_BUILDER);
            final RuntimePersistentEntity<T> persistentEntity = getEntity(operation.getRootEntity());
            StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
            if (!isSupportsBatchUpdate(persistentEntity, queryBuilder.dialect())) {
                return operation.split()
                        .stream()
                        .map(updateOp -> {
                            JdbcEntityOperations<T> op = new JdbcEntityOperations<>(persistentEntity, updateOp.getEntity());
                            updateOne(status.getConnection(), dbOperation, op, new OperationContext(annotationMetadata, repositoryType, queryBuilder.dialect()));
                            return op.entity;
                        })
                        .collect(Collectors.toList());
            }
            JdbcEntitiesOperations<T> op = new JdbcEntitiesOperations<>(persistentEntity, operation);
            updateInBatch(status.getConnection(), dbOperation, op, new OperationContext(annotationMetadata, repositoryType, queryBuilder.dialect()));
            return op.getEntities();
        });
    }

    @NonNull
    @Override
    public <T> T persist(@NonNull InsertOperation<T> operation) {
        final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
        final Class<?> repositoryType = operation.getRepositoryType();
        SqlQueryBuilder queryBuilder = queryBuilders.getOrDefault(repositoryType, DEFAULT_SQL_BUILDER);
        StoredSqlOperation dbOperation = new StoredQuerySqlOperation(queryBuilder, operation.getStoredQuery());
        return transactionOperations.executeWrite((status) -> {
            JdbcEntityOperations<T> op = new JdbcEntityOperations<>(getEntity(operation.getRootEntity()), operation.getEntity(), true);
            persistOne(status.getConnection(), dbOperation, op, new OperationContext(annotationMetadata, repositoryType, queryBuilder.dialect()));
            return op;
        }).entity;
    }

    @Nullable
    @Override
    public <T> T findOne(@NonNull Class<T> type, @NonNull Serializable id) {
        throw new UnsupportedOperationException("The findOne method by ID is not supported. Execute the SQL query directly");
    }

    @NonNull
    @Override
    public <T> Iterable<T> findAll(@NonNull PagedQuery<T> query) {
        throw new UnsupportedOperationException("The findAll method without an explicit query is not supported. Use findAll(PreparedQuery) instead");
    }

    @Override
    public <T> long count(PagedQuery<T> pagedQuery) {
        throw new UnsupportedOperationException("The count method without an explicit query is not supported. Use findAll(PreparedQuery) instead");
    }

    @NonNull
    @Override
    public <T> Stream<T> findStream(@NonNull PagedQuery<T> query) {
        throw new UnsupportedOperationException("The findStream method without an explicit query is not supported. Use findStream(PreparedQuery) instead");
    }

    @Override
    public <R> Page<R> findPage(@NonNull PagedQuery<R> query) {
        throw new UnsupportedOperationException("The findPage method without an explicit query is not supported. Use findPage(PreparedQuery) instead");
    }

    @NonNull
    public <T> Iterable<T> persistAll(@NonNull InsertBatchOperation<T> operation) {
        return transactionOperations.executeWrite(status -> {
            final AnnotationMetadata annotationMetadata = operation.getAnnotationMetadata();
            final Class<?> repositoryType = operation.getRepositoryType();
            SqlQueryBuilder sqlQueryBuilder = queryBuilders.getOrDefault(repositoryType, DEFAULT_SQL_BUILDER);
            StoredSqlOperation dbOperation = new StoredQuerySqlOperation(sqlQueryBuilder, operation.getStoredQuery());
            final RuntimePersistentEntity<T> persistentEntity = getEntity(operation.getRootEntity());
            OperationContext operationContext = new OperationContext(annotationMetadata, repositoryType, sqlQueryBuilder.dialect());
            if (!isSupportsBatchInsert(persistentEntity, sqlQueryBuilder.dialect())) {
                return operation.split().stream()
                        .map(persistOp -> {
                            JdbcEntityOperations<T> op = new JdbcEntityOperations<>(persistentEntity, persistOp.getEntity(), true);
                            persistOne(
                                    status.getConnection(),
                                    dbOperation,
                                    op,
                                    operationContext);
                            return op.entity;
                        })
                        .collect(Collectors.toList());
            } else {
                JdbcEntitiesOperations<T> op = new JdbcEntitiesOperations<>(persistentEntity, operation, true);
                persistInBatch(
                        status.getConnection(),
                        dbOperation,
                        op,
                        operationContext
                );
                return op.getEntities();
            }

        });
    }

    @Override
    @PreDestroy
    public void close() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @NonNull
    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    @NonNull
    @Override
    public Connection getConnection() {
        return transactionOperations.getConnection();
    }

    @NonNull
    @Override
    public <R> R execute(@NonNull ConnectionCallback<R> callback) {
        try {
            return callback.call(transactionOperations.getConnection());
        } catch (SQLException e) {
            throw new DataAccessException("Error executing SQL Callback: " + e.getMessage(), e);
        }
    }

    @NonNull
    @Override
    public <R> R prepareStatement(@NonNull String sql, @NonNull PreparedStatementCallback<R> callback) {
        ArgumentUtils.requireNonNull("sql", sql);
        ArgumentUtils.requireNonNull("callback", callback);
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Query: {}", sql);
        }
        try {
            R result = null;
            PreparedStatement ps = transactionOperations.getConnection().prepareStatement(sql);
            try {
                result = callback.call(ps);
                return result;
            } finally {
                if (!(result instanceof AutoCloseable)) {
                    ps.close();
                }
            }
        } catch (SQLException e) {
            throw new DataAccessException("Error preparing SQL statement: " + e.getMessage(), e);
        }
    }

    @NonNull
    @Override
    public <T> Stream<T> entityStream(@NonNull ResultSet resultSet, @NonNull Class<T> rootEntity) {
        return entityStream(resultSet, null, rootEntity);
    }

    @NonNull
    @Override
    public <E> E readEntity(@NonNull String prefix, @NonNull ResultSet resultSet, @NonNull Class<E> type) throws DataAccessException {
        return new SqlResultEntityTypeMapper<>(
                prefix,
                getEntity(type),
                columnNameResultSetReader,
                jsonCodec,
                conversionService).map(resultSet, type);
    }

    @NonNull
    @Override
    public <E, D> D readDTO(@NonNull String prefix, @NonNull ResultSet resultSet, @NonNull Class<E> rootEntity, @NonNull Class<D> dtoType) throws DataAccessException {
        return new DTOMapper<E, ResultSet, D>(
                getEntity(rootEntity),
                columnNameResultSetReader,
                jsonCodec,
                conversionService).map(resultSet, dtoType);
    }

    @NonNull
    @Override
    public <T> Stream<T> entityStream(@NonNull ResultSet resultSet, @Nullable String prefix, @NonNull Class<T> rootEntity) {
        ArgumentUtils.requireNonNull("resultSet", resultSet);
        ArgumentUtils.requireNonNull("rootEntity", rootEntity);
        TypeMapper<ResultSet, T> mapper = new SqlResultEntityTypeMapper<>(prefix, getEntity(rootEntity), columnNameResultSetReader, jsonCodec, conversionService);
        Iterable<T> iterable = () -> new Iterator<T>() {
            boolean nextCalled = false;

            @Override
            public boolean hasNext() {
                try {
                    if (!nextCalled) {
                        nextCalled = true;
                        return resultSet.next();
                    } else {
                        return nextCalled;
                    }
                } catch (SQLException e) {
                    throw new DataAccessException("Error retrieving next JDBC result: " + e.getMessage(), e);
                }
            }

            @Override
            public T next() {
                nextCalled = false;
                return mapper.map(resultSet, rootEntity);
            }
        };
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    @NonNull
    protected ResultConsumer.Context<ResultSet> newMappingContext(ResultSet rs) {
        return new ResultConsumer.Context<ResultSet>() {
            @Override
            public ResultSet getResultSet() {
                return rs;
            }

            @Override
            public ResultReader<ResultSet, String> getResultReader() {
                return columnNameResultSetReader;
            }

            @NonNull
            @Override
            public <E> E readEntity(String prefix, Class<E> type) throws DataAccessException {
                RuntimePersistentEntity<E> entity = getEntity(type);
                SqlResultEntityTypeMapper<ResultSet, E> mapper = new SqlResultEntityTypeMapper<>(
                        prefix,
                        entity,
                        columnNameResultSetReader,
                        jsonCodec,
                        conversionService);
                return mapper.map(rs, type);
            }

            @NonNull
            @Override
            public <E, D> D readDTO(@NonNull String prefix, @NonNull Class<E> rootEntity, @NonNull Class<D> dtoType) throws DataAccessException {
                RuntimePersistentEntity<E> entity = getEntity(rootEntity);
                TypeMapper<ResultSet, D> introspectedDataMapper = new DTOMapper<>(
                        entity,
                        columnNameResultSetReader,
                        jsonCodec,
                        conversionService);
                return introspectedDataMapper.map(rs, dtoType);
            }
        };
    }

    private final class JdbcEntityOperations<T> extends EntityOperations<T> {

        private final boolean insert;
        private final boolean hasGeneratedId;
        private T entity;
        private Integer rowsUpdated;
        private Map<QueryParameterBinding, Object> previousValues;

        private JdbcEntityOperations(RuntimePersistentEntity<T> persistentEntity, T entity) {
            this(persistentEntity, entity, false);
        }

        private JdbcEntityOperations(RuntimePersistentEntity<T> persistentEntity, T entity, boolean insert) {
            super(persistentEntity);
            this.insert = insert;
            this.hasGeneratedId = insert && persistentEntity.getIdentity() != null && persistentEntity.getIdentity().isGenerated();
            Objects.requireNonNull(entity, "Passed entity cannot be null");
            this.entity = entity;
        }

        @Override
        protected void cascadePre(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            entity = cascadeEntity(entity, persistentEntity, false, cascadeType, connection, operationContext);
        }

        @Override
        protected void cascadePost(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            entity = cascadeEntity(entity, persistentEntity, true, cascadeType, connection, operationContext);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues(DBOperation dbOperation) {
            previousValues = dbOperation.collectAutoPopulatedPreviousValues(persistentEntity, entity);
        }

        private PreparedStatement prepare(Connection connection, DBOperation dbOperation) throws SQLException {
            if (StoredSqlOperation.class.isInstance(dbOperation)) {
                ((StoredSqlOperation) dbOperation).checkForParameterToBeExpanded(persistentEntity, entity);
            }
            if (insert) {
                StoredSqlOperation sqlOperation = (StoredSqlOperation) dbOperation;
                Dialect dialect = sqlOperation.getDialect();
                if (hasGeneratedId && (dialect == Dialect.ORACLE || dialect == Dialect.SQL_SERVER)) {
                    return connection.prepareStatement(dbOperation.getQuery(), new String[]{persistentEntity.getIdentity().getPersistedName()});
                } else {
                    return connection.prepareStatement(dbOperation.getQuery(), hasGeneratedId ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
                }
            } else {
                return connection.prepareStatement(dbOperation.getQuery());
            }
        }

        @Override
        protected void executeUpdate(OpContext<Connection, PreparedStatement> context, Connection connection, DBOperation dbOperation) throws SQLException {
            try (PreparedStatement ps = prepare(connection, dbOperation)) {
                dbOperation.setParameters(context, connection, ps, persistentEntity, entity, previousValues);
                rowsUpdated = ps.executeUpdate();
                if (hasGeneratedId) {
                    try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                            Object id = columnIndexResultSetReader.readDynamic(generatedKeys, 1, identity.getDataType());
                            BeanProperty<T, Object> property = (BeanProperty<T, Object>) identity.getProperty();
                            entity = updateEntityId(property, entity, id);
                        } else {
                            throw new DataAccessException("Failed to generate ID for entity: " + entity);
                        }
                    }
                }
            }
        }

        @Override
        protected void executeUpdate(OpContext<Connection, PreparedStatement> context, Connection connection, DBOperation dbOperation, DBOperation2<Integer, Integer, SQLException> fn) throws SQLException {
            try (PreparedStatement ps = prepare(connection, dbOperation)) {
                dbOperation.setParameters(context, connection, ps, persistentEntity, entity, previousValues);
                int ru = ps.executeUpdate();
                fn.process(1, ru);
                rowsUpdated = ru;
            }
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
        protected void veto(Predicate<T> predicate) {
            throw new IllegalStateException("Not supported");
        }
    }

    private final class JdbcEntitiesOperations<T> extends EntitiesOperations<T> {

        private final List<Data> entities;
        private final boolean insert;
        private final boolean hasGeneratedId;
        private int rowsUpdated;

        private JdbcEntitiesOperations(RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
            this(persistentEntity, entities, false);
        }

        private JdbcEntitiesOperations(RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, boolean insert) {
            super(persistentEntity);
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
        protected void cascadePre(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            for (Data d : entities) {
                if (d.vetoed) {
                    continue;
                }
                d.entity = cascadeEntity(d.entity, persistentEntity, false, cascadeType, connection, operationContext);
            }
        }

        @Override
        protected void cascadePost(Relation.Cascade cascadeType, Connection connection, OperationContext operationContext) {
            for (Data d : entities) {
                if (d.vetoed) {
                    continue;
                }
                d.entity = cascadeEntity(d.entity, persistentEntity, true, cascadeType, connection, operationContext);
            }
        }

        @Override
        protected void collectAutoPopulatedPreviousValues(DBOperation sqlOperation) {
            for (Data d : entities) {
                if (d.vetoed) {
                    continue;
                }
                d.previousValues = sqlOperation.collectAutoPopulatedPreviousValues(persistentEntity, d.entity);
            }
        }

        private PreparedStatement prepare(Connection connection, DBOperation dbOperation) throws SQLException {
            if (insert) {
                StoredSqlOperation sqlOperation = (StoredSqlOperation) dbOperation;
                Dialect dialect = sqlOperation.getDialect();
                if (hasGeneratedId && (dialect == Dialect.ORACLE || dialect == Dialect.SQL_SERVER)) {
                    return connection.prepareStatement(dbOperation.getQuery(), new String[]{persistentEntity.getIdentity().getPersistedName()});
                } else {
                    return connection.prepareStatement(dbOperation.getQuery(), hasGeneratedId ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
                }
            } else {
                return connection.prepareStatement(dbOperation.getQuery());
            }
        }

        @Override
        protected void veto(Predicate<T> predicate) {
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

        private void setParameters(OpContext<Connection, PreparedStatement> context, Connection connection, PreparedStatement stmt, DBOperation sqlOperation) throws SQLException {
            for (Data d : entities) {
                if (d.vetoed) {
                    continue;
                }
                sqlOperation.setParameters(context, connection, stmt, persistentEntity, d.entity, d.previousValues);
                stmt.addBatch();
            }
        }

        @Override
        protected void executeUpdate(OpContext<Connection, PreparedStatement> context, Connection connection, DBOperation dbOperation) throws SQLException {
            try (PreparedStatement ps = prepare(connection, dbOperation)) {
                setParameters(context, connection, ps, dbOperation);
                rowsUpdated = Arrays.stream(ps.executeBatch()).sum();
                if (hasGeneratedId) {
                    RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                    List<Object> ids = new ArrayList<>();
                    try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                        while (generatedKeys.next()) {
                            ids.add(columnIndexResultSetReader.readDynamic(generatedKeys, 1, identity.getDataType()));
                        }
                    }
                    Iterator<Object> iterator = ids.iterator();
                    for (Data d : entities) {
                        if (d.vetoed) {
                            continue;
                        }
                        if (!iterator.hasNext()) {
                            throw new DataAccessException("Failed to generate ID for entity: " + d.entity);
                        } else {
                            Object id = iterator.next();
                            d.entity = updateEntityId((BeanProperty<T, Object>) identity.getProperty(), d.entity, id);
                        }
                    }
                } else {
                    rowsUpdated = Arrays.stream(ps.executeBatch()).sum();
                }
            }
        }

        @Override
        protected void executeUpdate(OpContext<Connection, PreparedStatement> context, Connection connection, DBOperation dbOperation, DBOperation2<Integer, Integer, SQLException> fn) throws SQLException {
            try (PreparedStatement ps = prepare(connection, dbOperation)) {
                setParameters(context, connection, ps, dbOperation);
                rowsUpdated = Arrays.stream(ps.executeBatch()).sum();
                int expected = (int) entities.stream().filter(d -> !d.vetoed).count();
                fn.process(expected, rowsUpdated);
            }
        }

        protected List<T> getEntities() {
            return entities.stream().map(d -> d.entity).collect(Collectors.toList());
        }

        class Data {
            T entity;
            Map<QueryParameterBinding, Object> previousValues;
            boolean vetoed = false;
        }
    }

    private static final class RuntimePersistentPropertyJdbcCC extends JdbcConversionContextImpl implements RuntimePersistentPropertyConversionContext {

        private final RuntimePersistentProperty<?> property;

        public RuntimePersistentPropertyJdbcCC(Connection connection, RuntimePersistentProperty<?> property) {
            super(ConversionContext.of(property.getArgument()), connection);
            this.property = property;
        }

        @Override
        public RuntimePersistentProperty<?> getRuntimePersistentProperty() {
            return property;
        }
    }

    private static final class ArgumentJdbcCC extends JdbcConversionContextImpl implements ArgumentConversionContext<Object> {

        private final Argument argument;

        public ArgumentJdbcCC(Connection connection, Argument argument) {
            super(ConversionContext.of(argument), connection);
            this.argument = argument;
        }

        @Override
        public Argument<Object> getArgument() {
            return argument;
        }
    }

    private static class JdbcConversionContextImpl extends AbstractConversionContext
            implements JdbcConversionContext {

        private final Connection connection;

        public JdbcConversionContextImpl(Connection connection) {
            this(ConversionContext.DEFAULT, connection);
        }

        public JdbcConversionContextImpl(ConversionContext conversionContext, Connection connection) {
            super(conversionContext);
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

    }

}
