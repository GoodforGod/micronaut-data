package io.micronaut.data.mongodb.operations;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.DeleteBatchOperation;
import io.micronaut.data.model.runtime.DeleteOperation;
import io.micronaut.data.model.runtime.InsertBatchOperation;
import io.micronaut.data.model.runtime.InsertOperation;
import io.micronaut.data.model.runtime.PagedQuery;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.UpdateBatchOperation;
import io.micronaut.data.model.runtime.UpdateOperation;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntitiesOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntityOperations;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.SyncCascadeOperations;
import io.micronaut.http.codec.MediaTypeCodec;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EachBean(MongoClient.class)
@Internal
public class DefaultMongoDbRepositoryOperations extends AbstractRepositoryOperations<ClientSession, Object>
        implements MongoDbRepositoryOperations, SyncCascadeOperations.SyncCascadeOperationsHelper<DefaultMongoDbRepositoryOperations.MongoDbOperationContext> {

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
            MongoCollection<T> collection = getCollection(persistentEntity);
            // TODO: correct UUID
            Serializable value = id instanceof UUID ? id.toString() : id;
            Bson eq = Filters.eq(value);

            return collection.find(clientSession, eq, type).first();
        }
    }

    @Override
    public <T, R> R findOne(PreparedQuery<T, R> preparedQuery) {
        return null;
    }

    @Override
    public <T> boolean exists(PreparedQuery<T, Boolean> preparedQuery) {
        return false;
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
        return null;
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

    private <T> MongoCollection<T> getCollection(RuntimePersistentEntity<T> persistentEntity) {
        return getDatabase().getCollection(persistentEntity.getPersistedName(), persistentEntity.getIntrospection().getBeanType());
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
    public void persistManyAssociationSync(MongoDbOperationContext ctx, RuntimeAssociation runtimeAssociation, Object value, RuntimePersistentEntity<Object> persistentEntity, Object child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    @Override
    public void persistManyAssociationBatchSync(MongoDbOperationContext ctx, RuntimeAssociation runtimeAssociation, Object value, RuntimePersistentEntity<Object> persistentEntity, Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    private <T> MongoDbEntityOperation<T> createMongoDbInsertOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, true) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity);
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
                MongoCollection<T> collection = getCollection(persistentEntity);
                Object id = persistentEntity.getIdentity().getProperty().get(entity);
                UpdateResult updateResult = collection.replaceOne(ctx.clientSession, Filters.eq(id), entity);
                modifiedCount = updateResult.getModifiedCount();
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbReplaceManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            MongoCollection<T> collection = getCollection(persistentEntity);

            @Override
            protected void execute() throws RuntimeException {
                for (Data d : entities) {
                    if (d.vetoed) {
                        continue;
                    }
                    Object id = persistentEntity.getIdentity().getProperty().get(d.entity);
                    UpdateResult updateResult = collection.replaceOne(ctx.clientSession, Filters.eq(id), d.entity);
                    modifiedCount += updateResult.getModifiedCount();
                }
            }
        };
    }

    private <T> MongoDbEntityOperation<T> createMongoDbDeleteOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbEntityOperation<T>(ctx, persistentEntity, entity, false) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity);
                Object id = persistentEntity.getIdentity().getProperty().get(entity);
                DeleteResult deleteResult = collection.deleteOne(ctx.clientSession, Filters.eq(id));
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
                        .map(Filters::eq)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), Filters::or));
                MongoCollection<T> collection = getCollection(persistentEntity);
                DeleteResult deleteResult = collection.deleteMany(ctx.clientSession, filter);
                modifiedCount = deleteResult.getDeletedCount();
            }
        };
    }

    private <T> MongoDbEntitiesOperation<T> createMongoDbInsertManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            @Override
            protected void execute() throws RuntimeException {
                MongoCollection<T> collection = getCollection(persistentEntity);
                InsertManyResult insertManyResult = collection.insertMany(entities.stream().map(d -> d.entity).collect(Collectors.toList()));
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
