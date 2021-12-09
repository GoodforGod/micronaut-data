package io.micronaut.data.document.mongodb.operations;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UnwindOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.beans.BeanWrapper;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.data.annotation.repeatable.JoinSpecifications;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.query.builder.sql.Dialect;
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
import io.micronaut.data.runtime.config.DataSettings;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntitiesOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntityOperations;
import io.micronaut.data.runtime.operations.internal.OperationContext;
import io.micronaut.data.runtime.operations.internal.SyncCascadeOperations;
import io.micronaut.http.codec.MediaTypeCodec;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@EachBean(MongoClient.class)
@Internal
public class DefaultMongoDbRepositoryOperations extends AbstractRepositoryOperations<ClientSession, Object>
        implements MongoDbRepositoryOperations, SyncCascadeOperations.SyncCascadeOperationsHelper<DefaultMongoDbRepositoryOperations.MongoDbOperationContext> {
    private static final Logger QUERY_LOG = DataSettings.QUERY_LOG;
    private final MongoClient mongoClient;
    private final SyncCascadeOperations<MongoDbOperationContext> cascadeOperations;

    /**
     * Default constructor.
     *
     * @param codecs                     The media type codecs
     * @param dateTimeProvider           The date time provider
     * @param runtimeEntityRegistry      The entity registry
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     * @param mongoClient
     */
    protected DefaultMongoDbRepositoryOperations(List<MediaTypeCodec> codecs,
                                                 DateTimeProvider<Object> dateTimeProvider,
                                                 RuntimeEntityRegistry runtimeEntityRegistry,
                                                 DataConversionService<?> conversionService,
                                                 AttributeConverterRegistry attributeConverterRegistry,
                                                 MongoClient mongoClient) {
        super(codecs, dateTimeProvider, runtimeEntityRegistry, conversionService, attributeConverterRegistry);
        this.mongoClient = mongoClient;
        this.cascadeOperations = new SyncCascadeOperations<>(conversionService, this);
    }

    @Override
    public <T> T findOne(Class<T> type, Serializable id) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            Bson eq = Utils.filterById(conversionService, persistentEntity, id);
            return getCollection(persistentEntity, type).find(clientSession, eq, type).first();
        }
    }

    @Override
    public <T, R> R findOne(PreparedQuery<T, R> preparedQuery) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            Class<T> type = preparedQuery.getRootEntity();
            Class<R> resultType = preparedQuery.getResultType();
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            return getCollection(persistentEntity, resultType).find(clientSession, type).limit(1).map(v -> (R) v).first();
        }
    }

    @Override
    public <T> boolean exists(PreparedQuery<T, Boolean> preparedQuery) {
        Class<T> type = preparedQuery.getRootEntity();
        RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
        Bson filter = getFilter(preparedQuery, persistentEntity);
        try (ClientSession clientSession = mongoClient.startSession()) {
            boolean b = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                    .find(clientSession, type)
                    .limit(1)
                    .filter(filter)
                    .iterator()
                    .hasNext();
            return b;
        }
    }

    private <T> Bson getFilter(PreparedQuery<T, Boolean> preparedQuery, RuntimePersistentEntity<T> persistentEntity) {
        String query = preparedQuery.getQuery();
        BsonDocument bsonDocument = BsonDocument.parse(query);
        bsonDocument = (BsonDocument) replaceQueryParameters(null, bsonDocument, preparedQuery, persistentEntity);
        return bsonDocument;
    }

    private <T> BsonValue replaceQueryParameters(@Nullable String key,
                                                 BsonDocument document,
                                                 PreparedQuery<?, ?> preparedQuery,
                                                 RuntimePersistentEntity<T> persistentEntity) {
        BsonInt32 queryParameterIndex = document.getInt32("$qpidx", null);
        if (queryParameterIndex != null) {
            int index = queryParameterIndex.getValue();
            return getValue(key, index, preparedQuery.getQueryBindings().get(index), preparedQuery, persistentEntity);
        }
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            BsonValue value = entry.getValue();
            if (value instanceof BsonDocument) {
                BsonValue newValue = replaceQueryParameters(entry.getKey(), (BsonDocument) value, preparedQuery, persistentEntity);
                if (newValue != value) {
                    entry.setValue(newValue);
                }
            }
        }
        return document;
    }

    private <T> BsonValue getValue(@Nullable String key,
                                   int index,
                                   QueryParameterBinding queryParameterBinding,
                                   PreparedQuery<?, ?> preparedQuery,
                                   RuntimePersistentEntity<T> persistentEntity) {
        Class<?> parameterConverter = queryParameterBinding.getParameterConverterClass();
        Object value;
        if (queryParameterBinding.getParameterIndex() != -1) {
            value = resolveParameterValue(queryParameterBinding, preparedQuery.getParameterArray());
        } else if (queryParameterBinding.isAutoPopulated()) {
            PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
            RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
            Object previousValue = null;
            QueryParameterBinding previousPopulatedValueParameter = queryParameterBinding.getPreviousPopulatedValueParameter();
            if (previousPopulatedValueParameter != null) {
                if (previousPopulatedValueParameter.getParameterIndex() == -1) {
                    throw new IllegalStateException("Previous value parameter cannot be bind!");
                }
                previousValue = resolveParameterValue(previousPopulatedValueParameter, preparedQuery.getParameterArray());
            }
//                value = context.getRuntimeEntityRegistry().autoPopulateRuntimeProperty(persistentProperty, previousValue);
//                value = context.convert(connection, value, persistentProperty);
            value = null;
            parameterConverter = null;
        } else {
            throw new IllegalStateException("Invalid query [" + "]. Unable to establish parameter value for parameter at position: " + (index + 1));
        }

        DataType dataType = queryParameterBinding.getDataType();
        List<Object> values = expandValue(value, dataType);
        if (values != null && values.isEmpty()) {
            // Empty collections / array should always set at least one value
            value = null;
            values = null;
        }
        if (values == null) {
            if (parameterConverter != null) {
                int parameterIndex = queryParameterBinding.getParameterIndex();
                Argument<?> argument = parameterIndex > -1 ? preparedQuery.getArguments()[parameterIndex] : null;
//                    value = context.convert(parameterConverter, connection, value, argument);
            }
//                context.setStatementParameter(stmt, index++, dataType, value, dialect);
//            appendValue(q, value);
            if (value instanceof String) {
                PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
                RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
                if (persistentProperty.getOwner().getIdentity() == persistentProperty && persistentProperty.getType() == String.class && persistentProperty.isGenerated()) {
                    return new BsonObjectId(new ObjectId((String) value));
                }
            }
            return Utils.toBsonValue(conversionService, value);
        } else {
            return null;
//            Iterator<Object> iterator = values.iterator();
//            while (iterator.hasNext()) {
//                if (parameterConverter != null) {
//                    int parameterIndex = queryParameterBinding.getParameterIndex();
//                    Argument<?> argument = parameterIndex > -1 ? preparedQuery.getArguments()[parameterIndex] : null;
////                        v = context.convert(parameterConverter, connection, v, argument);
//                }
//                appendValue(q, iterator.next());
//                if (iterator.hasNext()) {
//                    q.append(", ");
//                }
////                    context.setStatementParameter(stmt, index++, dataType, v, dialect);
//            }
        }
    }

    private <T> PersistentPropertyPath getRequiredPropertyPath(QueryParameterBinding queryParameterBinding, RuntimePersistentEntity<T> persistentEntity) {
        String[] propertyPath = queryParameterBinding.getRequiredPropertyPath();
        PersistentPropertyPath pp = persistentEntity.getPropertyPath(propertyPath);
        if (pp == null) {
            throw new IllegalStateException("Cannot find auto populated property: " + String.join(".", propertyPath));
        }
        return pp;
    }

    private void appendValue(StringBuilder q, Object value) {
        if (value == null) {
            q.append("null");
        } else if (value instanceof Number) {
            q.append(value);
        } else if (value instanceof Boolean) {
            q.append(((Boolean) value).toString().toLowerCase(Locale.ROOT));
        } else {
//            q.append('"').append(value).append('"');
            q.append("{ \"$oid\": \"").append(value).append("\" }");
        }
    }

    List<Object> expandValue(Object value, DataType dataType) {
        // Special case for byte array, we want to support a list of byte[] convertible values
        if (value == null || dataType != null && dataType.isArray() && dataType != DataType.BYTE_ARRAY || value instanceof byte[]) {
            // not expanded
            return null;
        } else if (value instanceof Iterable) {
            return (List<Object>) CollectionUtils.iterableToList((Iterable<?>) value);
        } else if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            if (len == 0) {
                return Collections.emptyList();
            } else {
                List<Object> list = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    Object o = Array.get(value, j);
                    list.add(o);
                }
                return list;
            }
        } else {
            // not expanded
            return null;
        }
    }

    private Object resolveParameterValue(QueryParameterBinding queryParameterBinding, Object[] parameterArray) {
        Object value;
        value = parameterArray[queryParameterBinding.getParameterIndex()];
        String[] parameterBindingPath = queryParameterBinding.getParameterBindingPath();
        if (parameterBindingPath != null) {
            for (String prop : parameterBindingPath) {
                if (value == null) {
                    break;
                }
                value = BeanWrapper.getWrapper(value).getRequiredProperty(prop, Argument.OBJECT_ARGUMENT);
            }
        }
        return value;
    }

    @Override
    public <T> Iterable<T> findAll(PagedQuery<T> query) {
        return null;
    }

    @Override
    public <T> long count(PagedQuery<T> pagedQuery) {
        return 0;
    }

    @Override
    public <T, R> Iterable<R> findAll(PreparedQuery<T, R> preparedQuery) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            Class<T> type = preparedQuery.getRootEntity();
            Class<R> resultType = preparedQuery.getResultType();
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            if (preparedQuery.isCount()) {
                long count = getCollection(persistentEntity, BsonDocument.class).countDocuments(clientSession);
                return Collections.singletonList(conversionService.convertRequired(count, resultType));
            }
            AnnotationValue<JoinSpecifications> joins = preparedQuery.getAnnotationMetadata().getAnnotation(JoinSpecifications.class);
            List<String> joined = new ArrayList<>();
            if (joins != null) {
                for (AnnotationValue<Annotation> join : joins.getAnnotations("value")) {
                    join.stringValue().ifPresent(joined::add);
                }
            }
            List<Bson> pipeline = new ArrayList<>();
            for (String join : joined) {
                StringJoiner fullPath = new StringJoiner(".");
                String prev = null;
                for (String path : StringUtils.splitOmitEmptyStrings(join, '.')) {
                    fullPath.add(path);
                    String thisPath = fullPath.toString();
                    PersistentPropertyPath propertyPath = persistentEntity.getPropertyPath(thisPath);
                    PersistentProperty property = propertyPath.getProperty();
                    if (!(property instanceof RuntimeAssociation)) {
                        continue;
                    }
                    RuntimeAssociation<Object> runtimeAssociation = (RuntimeAssociation) property;
                    if (runtimeAssociation.isForeignKey()) {
                        pipeline.add(Aggregates.lookup(
                                runtimeAssociation.getAssociatedEntity().getPersistedName(),
                                (prev == null) ? "_id" : prev + "._id",
                                runtimeAssociation.getInverseSide().get().getName() + "._id",
                                thisPath)
                        );
                    } else {
                        pipeline.add(Aggregates.lookup(
                                runtimeAssociation.getAssociatedEntity().getPersistedName(),
                                thisPath + "._id",
                                "_id",
                                thisPath)
                        );
                        pipeline.add(Aggregates.unwind("$" + thisPath, new UnwindOptions().preserveNullAndEmptyArrays(true)));
                    }
                    prev = thisPath;
                }
            }
            if (pipeline.isEmpty()) {
                Spliterator<R> spliterator = getCollection(persistentEntity, resultType).find(clientSession, resultType).spliterator();
                return StreamSupport.stream(spliterator, false).collect(Collectors.toList());
            }
            Spliterator<R> spliterator = getCollection(persistentEntity, resultType).aggregate(clientSession, pipeline, resultType).spliterator();
            return StreamSupport.stream(spliterator, false).collect(Collectors.toList());
        }
    }

    @Override
    public <T, R> Stream<R> findStream(PreparedQuery<T, R> preparedQuery) {
        return null;
    }

    @Override
    public <T> Stream<T> findStream(PagedQuery<T> query) {
        return null;
    }

    @Override
    public <R> Page<R> findPage(PagedQuery<R> query) {
        return null;
    }

    @Override
    public <T> T persist(InsertOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return persistOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        }
    }

    @Override
    public <T> Iterable<T> persistAll(InsertBatchOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return persistBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()), null);
        }
    }

    @Override
    public <T> T update(UpdateOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return updateOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        }
    }

    @Override
    public <T> Iterable<T> updateAll(UpdateBatchOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return updateBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        }
    }

    @Override
    public <T> int delete(DeleteOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoDbEntityOperation<T> op = createMongoDbDeleteOneOperation(ctx, persistentEntity, operation.getEntity());
            op.delete();
            return (int) op.modifiedCount;
        }
    }

    @Override
    public <T> Optional<Number> deleteAll(DeleteBatchOperation<T> operation) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoDbEntitiesOperation<T> op = createMongoDbDeleteManyOperation(ctx, persistentEntity, operation);
            op.delete();
            return Optional.of(op.modifiedCount);
        }
    }


    @Override
    public Optional<Number> executeUpdate(PreparedQuery<?, Number> preparedQuery) {
        return Optional.empty();
    }

    @Override
    protected ConversionContext createTypeConversionContext(ClientSession connection, RuntimePersistentProperty<?> property, Argument<?> argument) {
        return null;
    }

    @Override
    public void setStatementParameter(Object preparedStatement, int index, DataType dataType, Object value, Dialect dialect) {

    }

    private <T, R> MongoCollection<R> getCollection(RuntimePersistentEntity<T> persistentEntity, Class<R> resultType) {
        return getDatabase().getCollection(persistentEntity.getPersistedName(), resultType);
    }

    private MongoDatabase getDatabase() {
        return mongoClient.getDatabase("default");
    }

    @Override
    public boolean supportsBatch(MongoDbOperationContext mongoDbOperationContext, RuntimePersistentEntity<?> persistentEntity) {
        return true;
    }

    @Override
    public <T> T persistOne(MongoDbOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbEntityOperation<T> op = createMongoDbInsertOneOperation(ctx, persistentEntity, value);
        op.persist();
        return op.getEntity();
    }

    @Override
    public <T> List<T> persistBatch(MongoDbOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity, Predicate<T> predicate) {
        MongoDbEntitiesOperation<T> op = createMongoDbInsertManyOperation(ctx, persistentEntity, values);
        if (predicate != null) {
            op.veto(predicate);
        }
        op.persist();
        return op.getEntities();
    }

    @Override
    public <T> T updateOne(MongoDbOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbEntityOperation<T> op = createMongoDbReplaceOneOperation(ctx, persistentEntity, value);
        op.update();
        return op.getEntity();
    }

    private <T> List<T> updateBatch(MongoDbOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbEntitiesOperation<T> op = createMongoDbReplaceManyOperation(ctx, persistentEntity, values);
        op.update();
        return op.getEntities();
    }

    @Override
    public void persistManyAssociation(MongoDbOperationContext ctx, RuntimeAssociation runtimeAssociation, Object value, RuntimePersistentEntity<Object> persistentEntity, Object child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    @Override
    public void persistManyAssociationBatch(MongoDbOperationContext ctx, RuntimeAssociation runtimeAssociation, Object value, RuntimePersistentEntity<Object> persistentEntity, Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    private <T> MongoDbEntityOperation<T> createMongoDbInsertOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, true) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType());
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'insertOne' with entity: {}", entity);
                }
                InsertOneResult insertOneResult = collection.insertOne(ctx.clientSession, entity);
                BsonValue insertedId = insertOneResult.getInsertedId();
                BeanProperty<T, Object> property = (BeanProperty<T, Object>) persistentEntity.getIdentity().getProperty();
                if (property.get(entity) == null) {
                    entity = updateEntityId(property, entity, insertedId);
                }
            }
        };
    }

    private <T> MongoDbEntityOperation<T> createMongoDbReplaceOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, false) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType());
                Object id = persistentEntity.getIdentity().getProperty().get(entity);
                Bson filter = Utils.filterById(conversionService, persistentEntity, id);
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'replaceOne' with filter: {}", filter.toBsonDocument().toJson());
                }
                UpdateResult updateResult = collection.replaceOne(ctx.clientSession, filter, entity);
                modifiedCount = updateResult.getModifiedCount();
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbReplaceManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            @Override
            protected void execute() throws RuntimeException {
                for (Data d : entities) {
                    if (d.vetoed) {
                        continue;
                    }
                    Object id = persistentEntity.getIdentity().getProperty().get(d.entity);
                    Bson filter = Utils.filterById(conversionService, persistentEntity, id);
                    if (QUERY_LOG.isDebugEnabled()) {
                        QUERY_LOG.debug("Executing Mongo 'replaceOne' with filter: {}", filter.toBsonDocument().toJson());
                    }
                    UpdateResult updateResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType()).replaceOne(ctx.clientSession, filter, d.entity);
                    modifiedCount += updateResult.getModifiedCount();
                }
            }
        };
    }

    private <T> MongoDbEntityOperation<T> createMongoDbDeleteOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, false) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType());
                Object id = persistentEntity.getIdentity().getProperty().get(entity);
                Bson filter = Utils.filterById(conversionService, persistentEntity, id);
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'deleteOne' with filter: {}", filter.toBsonDocument().toJson());
                }
                DeleteResult deleteResult = collection.deleteOne(ctx.clientSession, filter);
                modifiedCount = deleteResult.getDeletedCount();
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbDeleteManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            @Override
            protected void execute() throws RuntimeException {
                BeanProperty<T, ?> idProperty = persistentEntity.getIdentity().getProperty();
                Bson filter = entities.stream()
                        .map(d -> idProperty.get(d.entity))
                        .map(id -> Utils.filterById(conversionService, persistentEntity, id))
                        .collect(Collectors.collectingAndThen(Collectors.toList(), Filters::or));
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'deleteMany' with filter: {}", filter.toBsonDocument().toJson());
                }
                DeleteResult deleteResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .deleteMany(ctx.clientSession, filter);
                modifiedCount = deleteResult.getDeletedCount();
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbInsertManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, true) {

            @Override
            protected void execute() throws RuntimeException {
                List<T> toInsert = entities.stream().map(d -> d.entity).collect(Collectors.toList());
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'insertMany' with entities: {}", toInsert);
                }
                InsertManyResult insertManyResult = getCollection(persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .insertMany(toInsert);
                if (hasGeneratedId) {
                    Map<Integer, BsonValue> insertedIds = insertManyResult.getInsertedIds();
                    RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                    BeanProperty<T, Object> IdProperty = (BeanProperty<T, Object>) identity.getProperty();
                    int index = 0;
                    for (Data d : entities) {
                        if (!d.vetoed) {
                            BsonValue id = insertedIds.get(index);
                            if (id == null) {
                                throw new DataAccessException("Failed to generate ID for entity: " + d.entity);
                            }
                            d.entity = updateEntityId(IdProperty, d.entity, id);
                        }
                        index++;
                    }
                }
            }
        };
    }

    abstract class MongoDbEntityOperation<T> extends AbstractSyncEntityOperations<MongoDbOperationContext, T, RuntimeException> {

        protected long modifiedCount;

        /**
         * Create a new instance.
         *
         * @param ctx
         * @param persistentEntity The RuntimePersistentEntity
         * @param entity
         */
        protected MongoDbEntityOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity, boolean insert) {
            super(ctx,
                    DefaultMongoDbRepositoryOperations.this.cascadeOperations,
                    DefaultMongoDbRepositoryOperations.this.entityEventRegistry,
                    persistentEntity,
                    DefaultMongoDbRepositoryOperations.this.conversionService,
                    entity,
                    insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }
    }

    abstract class MongoDbEntitiesOperation<T> extends AbstractSyncEntitiesOperations<MongoDbOperationContext, T, RuntimeException> {

        protected long modifiedCount;

        protected MongoDbEntitiesOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, boolean insert) {
            super(ctx,
                    DefaultMongoDbRepositoryOperations.this.cascadeOperations,
                    DefaultMongoDbRepositoryOperations.this.conversionService,
                    DefaultMongoDbRepositoryOperations.this.entityEventRegistry,
                    persistentEntity,
                    entities,
                    insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }

    }

    protected static class MongoDbOperationContext extends OperationContext {

        private final ClientSession clientSession;

        public MongoDbOperationContext(ClientSession clientSession, Class<?> repositoryType, AnnotationMetadata annotationMetadata) {
            super(annotationMetadata, repositoryType);
            this.clientSession = clientSession;
        }
    }
}
