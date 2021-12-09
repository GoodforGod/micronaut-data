package io.micronaut.data.document.mongodb.operations;

import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
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
import io.micronaut.data.operations.reactive.ReactorReactiveRepositoryOperations;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.operations.internal.AbstractReactiveEntitiesOperations;
import io.micronaut.data.runtime.operations.internal.AbstractReactiveEntityOperations;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.OperationContext;
import io.micronaut.data.runtime.operations.internal.ReactiveCascadeOperations;
import io.micronaut.http.codec.MediaTypeCodec;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@EachBean(MongoClient.class)
@Internal
public class DefaultReactiveMongoDbRepositoryOperations extends AbstractRepositoryOperations<ClientSession, Object>
        implements MongoDbReactiveRepositoryOperations,
        ReactorReactiveRepositoryOperations,
        ReactiveCascadeOperations.ReactiveCascadeOperationsHelper<DefaultReactiveMongoDbRepositoryOperations.MongoDbOperationContext> {

    private final MongoClient mongoClient;
    private final ReactiveCascadeOperations<MongoDbOperationContext> cascadeOperations;

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
    protected DefaultReactiveMongoDbRepositoryOperations(List<MediaTypeCodec> codecs,
                                                         DateTimeProvider<Object> dateTimeProvider,
                                                         RuntimeEntityRegistry runtimeEntityRegistry,
                                                         DataConversionService<?> conversionService,
                                                         AttributeConverterRegistry attributeConverterRegistry,
                                                         MongoClient mongoClient) {
        super(codecs, dateTimeProvider, runtimeEntityRegistry, conversionService, attributeConverterRegistry);
        this.mongoClient = mongoClient;
        this.cascadeOperations = new ReactiveCascadeOperations<>(conversionService, this);
    }

    @Override
    public <T> Mono<T> findOne(Class<T> type, Serializable id) {
        return Mono.from(mongoClient.startSession()).flatMap(clientSession -> {
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            MongoCollection<T> collection = getCollection(persistentEntity);
            // TODO: correct UUID
            Serializable value = id instanceof UUID ? id.toString() : id;
            Bson eq = Filters.eq(value);

            return Mono.from(collection.find(clientSession, eq, type).first());
        });
    }

    @Override
    public <T> Mono<Boolean> exists(PreparedQuery<T, Boolean> preparedQuery) {
        return null;
    }

    @Override
    public <T, R> Mono<R> findOne(PreparedQuery<T, R> preparedQuery) {
        return null;
    }

    @Override
    public <T> Mono<T> findOptional(Class<T> type, Serializable id) {
        return null;
    }

    @Override
    public <T, R> Mono<R> findOptional(PreparedQuery<T, R> preparedQuery) {
        return null;
    }

    @Override
    public <T> Flux<T> findAll(PagedQuery<T> pagedQuery) {
        return null;
    }

    @Override
    public <T> Mono<Long> count(PagedQuery<T> pagedQuery) {
        return null;
    }

    @Override
    public <T, R> Flux<R> findAll(PreparedQuery<T, R> preparedQuery) {
        return null;
    }

    @Override
    public <T> Mono<T> persist(InsertOperation<T> operation) {
        return Mono.from(mongoClient.startSession()).flatMap(clientSession -> {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return persistOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        });
    }

    @Override
    public <T> Flux<T> persistAll(InsertBatchOperation<T> operation) {
        return Mono.from(mongoClient.startSession()).flatMapMany(clientSession -> {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return persistBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()), null);
        });
    }

    @Override
    public <T> Mono<T> update(UpdateOperation<T> operation) {
        return Mono.from(mongoClient.startSession()).flatMap(clientSession -> {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return updateOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        });
    }

    @Override
    public <T> Flux<T> updateAll(UpdateBatchOperation<T> operation) {
        return Mono.from(mongoClient.startSession()).flatMapMany(clientSession -> {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            return updateBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        });
    }

    @Override
    public <T> Mono<Number> delete(DeleteOperation<T> operation) {
        return Mono.from(mongoClient.startSession()).flatMap(clientSession -> {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoDbReactiveEntityOperation<T> op = createMongoDbDeleteOneOperation(ctx, persistentEntity, operation.getEntity());
            op.delete();
            return op.getRowsUpdated();
        });
    }

    @Override
    public <T> Mono<Number> deleteAll(DeleteBatchOperation<T> operation) {
        return Mono.from(mongoClient.startSession()).flatMap(clientSession -> {
            MongoDbOperationContext ctx = new MongoDbOperationContext(clientSession, operation.getRepositoryType(), operation.getAnnotationMetadata());
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoDbReactiveEntitiesOperation<T> op = createMongoDbDeleteManyOperation(ctx, persistentEntity, operation);
            op.delete();
            return op.getRowsUpdated();
        });
    }

    @Override
    public <R> Mono<Page<R>> findPage(PagedQuery<R> pagedQuery) {
        return null;
    }

    @Override
    public Mono<Number> executeUpdate(PreparedQuery<?, Number> preparedQuery) {
        return null;
    }

    @Override
    public Mono<Number> executeDelete(PreparedQuery<?, Number> preparedQuery) {
        return null;
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
    public <T> Mono<T> persistOne(MongoDbOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbReactiveEntityOperation<T> op = createMongoDbInsertOneOperation(ctx, persistentEntity, value);
        op.persist();
        return op.getEntity();
    }

    @Override
    public <T> Flux<T> persistBatch(MongoDbOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity, Predicate<T> predicate) {
        MongoDbReactiveEntitiesOperation<T> op = createMongoDbInsertManyOperation(ctx, persistentEntity, values);
        if (predicate != null) {
            op.veto(predicate);
        }
        op.persist();
        return op.getEntities();
    }

    @Override
    public <T> Mono<T> updateOne(MongoDbOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbReactiveEntityOperation<T> op = createMongoDbReplaceOneOperation(ctx, persistentEntity, value);
        op.update();
        return op.getEntity();
    }

    private <T> Flux<T> updateBatch(MongoDbOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity) {
        MongoDbReactiveEntitiesOperation<T> op = createMongoDbReplaceManyOperation(ctx, persistentEntity, values);
        op.update();
        return op.getEntities();
    }

    @Override
    public Mono<Void> persistManyAssociation(MongoDbOperationContext ctx,
                                             RuntimeAssociation runtimeAssociation,
                                             Object value, RuntimePersistentEntity<Object> persistentEntity,
                                             Object child, RuntimePersistentEntity<Object> childPersistentEntity) {
        throw new IllegalStateException();
    }

    @Override
    public Mono<Void> persistManyAssociationBatch(MongoDbOperationContext ctx,
                                                  RuntimeAssociation runtimeAssociation,
                                                  Object value, RuntimePersistentEntity<Object> persistentEntity,
                                                  Iterable<Object> child, RuntimePersistentEntity<Object> childPersistentEntity,
                                                  Predicate<Object> veto) {
        throw new IllegalStateException();
    }

    private <T> MongoDbReactiveEntityOperation<T> createMongoDbInsertOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbReactiveEntityOperation<T>(ctx, persistentEntity, entity, true) {

            @Override
            protected void execute() throws RuntimeException {
                data = data.flatMap(d -> Mono.from(getCollection(persistentEntity).insertOne(ctx.clientSession, d.entity)).map(insertOneResult -> {
                    BsonValue insertedId = insertOneResult.getInsertedId();
                    BeanProperty<T, Object> property = (BeanProperty<T, Object>) persistentEntity.getIdentity().getProperty();
                    if (property.get(entity) == null) {
                        d.entity = updateEntityId(property, entity, insertedId);
                    }
                    return d;
                }));
            }
        };
    }

    private <T> MongoDbReactiveEntityOperation<T> createMongoDbReplaceOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbReactiveEntityOperation<T>(ctx, persistentEntity, entity, false) {

            @Override
            protected void execute() throws RuntimeException {
                data = data.flatMap(d -> {
                    Object id = persistentEntity.getIdentity().getProperty().get(entity);
                    return Mono.from(getCollection(persistentEntity).replaceOne(ctx.clientSession, Filters.eq(id), d.entity)).map(updateResult -> {
                        d.rowsUpdated = (int) updateResult.getModifiedCount();
                        return d;
                    });
                });
            }
        };
    }

    private <T> MongoDbReactiveEntityOperation<T> createMongoDbDeleteOneOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoDbReactiveEntityOperation<T>(ctx, persistentEntity, entity, false) {

            @Override
            protected void execute() throws RuntimeException {
                data = data.flatMap(d -> {
                    Object id = persistentEntity.getIdentity().getProperty().get(entity);
                    return Mono.from(getCollection(persistentEntity).deleteOne(ctx.clientSession, Filters.eq(id))).map(deleteResult -> {
                        d.rowsUpdated = (int) deleteResult.getDeletedCount();
                        return d;
                    });
                });
            }
        };
    }

    private <T> MongoDbReactiveEntitiesOperation<T> createMongoDbReplaceManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbReactiveEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            @Override
            protected void execute() throws RuntimeException {
                Flux<Tuple2<Data, Long>> modified = entities.flatMap(d -> {
                    if (d.vetoed) {
                        return Mono.just(Tuples.of(d, 0L));
                    }
                    Object id = persistentEntity.getIdentity().getProperty().get(d.entity);
                    return Mono.from(getCollection(persistentEntity).replaceOne(ctx.clientSession, Filters.eq(id), d.entity))
                            .map(updateResult -> Tuples.of(d, updateResult.getModifiedCount()));
                }).cache();
                entities = modified.map(Tuple2::getT1);
                rowsUpdated = modified.reduce(0, (sum, t) -> sum + t.getT2().intValue());
            }
        };
    }

    private <T> MongoDbReactiveEntitiesOperation<T> createMongoDbDeleteManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbReactiveEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            @Override
            protected void execute() throws RuntimeException {
                Mono<Tuple2<List<Data>, Long>> deleted = entities.collectList().flatMap(data -> {
                    BeanProperty<T, ?> idProperty = persistentEntity.getIdentity().getProperty();

                    Bson filter = data.stream()
                            .filter(d -> !d.vetoed)
                            .map(d -> idProperty.get(d.entity))
                            .map(Filters::eq)
                            .collect(Collectors.collectingAndThen(Collectors.toList(), Filters::or));

                    return Mono.from(getCollection(persistentEntity).deleteMany(ctx.clientSession, filter))
                            .map(deleteResult -> Tuples.of(data, deleteResult.getDeletedCount()));
                }).cache();
                entities = deleted.flatMapMany(t -> Flux.fromIterable(t.getT1()));
                rowsUpdated = deleted.map(t -> t.getT2().intValue());
            }
        };
    }

    private <T> MongoDbReactiveEntitiesOperation<T> createMongoDbInsertManyOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoDbReactiveEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            @Override
            protected void execute() throws RuntimeException {
                entities = entities.collectList().flatMapMany(data -> {
                    List<T> toInsert = data.stream()
                            .filter(d -> !d.vetoed)
                            .map(d -> d.entity)
                            .collect(Collectors.toList());

                    return Mono.from(getCollection(persistentEntity).insertMany(toInsert)).flatMapMany(insertManyResult -> {
                        if (hasGeneratedId) {
                            Map<Integer, BsonValue> insertedIds = insertManyResult.getInsertedIds();
                            RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                            BeanProperty<T, Object> IdProperty = (BeanProperty<T, Object>) identity.getProperty();
                            int index = 0;
                            for (Data d : data) {
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
                        return Flux.fromIterable(data);
                    });

                });
            }
        };
    }

    abstract class MongoDbReactiveEntityOperation<T> extends AbstractReactiveEntityOperations<MongoDbOperationContext, T, RuntimeException> {

        /**
         * Create a new instance.
         *
         * @param ctx
         * @param persistentEntity The RuntimePersistentEntity
         * @param entity
         */
        protected MongoDbReactiveEntityOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity, boolean insert) {
            super(ctx,
                    DefaultReactiveMongoDbRepositoryOperations.this.cascadeOperations,
                    DefaultReactiveMongoDbRepositoryOperations.this.conversionService,
                    DefaultReactiveMongoDbRepositoryOperations.this.entityEventRegistry,
                    persistentEntity,
                    entity,
                    insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }
    }

    abstract class MongoDbReactiveEntitiesOperation<T> extends AbstractReactiveEntitiesOperations<MongoDbOperationContext, T, RuntimeException> {

        protected MongoDbReactiveEntitiesOperation(MongoDbOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, boolean insert) {
            super(ctx,
                    DefaultReactiveMongoDbRepositoryOperations.this.cascadeOperations,
                    DefaultReactiveMongoDbRepositoryOperations.this.conversionService,
                    DefaultReactiveMongoDbRepositoryOperations.this.entityEventRegistry,
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
