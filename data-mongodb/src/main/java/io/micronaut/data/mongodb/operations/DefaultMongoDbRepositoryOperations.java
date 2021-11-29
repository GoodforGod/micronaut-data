package io.micronaut.data.mongodb.operations;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.InsertOneResult;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventContext;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.DeleteBatchOperation;
import io.micronaut.data.model.runtime.DeleteOperation;
import io.micronaut.data.model.runtime.InsertOperation;
import io.micronaut.data.model.runtime.PagedQuery;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.UpdateOperation;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.data.runtime.operations.internal.DBOperation;
import io.micronaut.data.runtime.operations.internal.OpContext;
import io.micronaut.http.codec.MediaTypeCodec;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

@EachBean(MongoClient.class)
@Internal
public class DefaultMongoDbRepositoryOperations extends AbstractRepositoryOperations<ClientSession, Object, RuntimeException> implements MongoDbRepositoryOperations {

    private final MongoClient mongoClient;

    /**
     * Default constructor.
     *  @param codecs                     The media type codecs
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
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoCollection<T> collection = getCollection(persistentEntity);

            MongoDbOperation<T> op = new MongoDbOperation<T>(persistentEntity, operation.getEntity()) {

                @Override
                protected void executeUpdate(OpContext<ClientSession, Object> context, ClientSession session) throws RuntimeException {
                    InsertOneResult insertOneResult = collection.insertOne(session, entity);
                    BsonValue insertedId = insertOneResult.getInsertedId();
                    BeanProperty<T, Object> property = (BeanProperty<T, Object>) persistentEntity.getIdentity().getProperty();
                    if (property.get(entity) == null) {
                        entity = updateEntityId(property, entity, insertedId);
                    }
                }
            };

            persistOne(clientSession, op, new OperationContext(operation.getAnnotationMetadata(), operation.getRepositoryType(), Dialect.ANSI));

            return op.getEntity();
        }
    }

    @Override
    public <T> T update(UpdateOperation<T> operation) {
        return null;
    }

    @Override
    public Optional<Number> executeUpdate(PreparedQuery<?, Number> preparedQuery) {
        return Optional.empty();
    }

    @Override
    public <T> int delete(DeleteOperation<T> operation) {
        return 0;
    }

    @Override
    public <T> Optional<Number> deleteAll(DeleteBatchOperation<T> operation) {
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

    abstract class MongoDbOperation<T> extends EntityOperations<T> {

        protected T entity;

        /**
         * Create a new instance.
         *
         * @param persistentEntity The RuntimePersistentEntity
         * @param entity
         */
        protected MongoDbOperation(RuntimePersistentEntity<T> persistentEntity, T entity) {
            super(persistentEntity);
            this.entity = entity;
        }

        @Override
        protected DBOperation getDbOperation() {
            throw new IllegalStateException();
        }

        @Override
        protected String debug() {
            return "";
        }

        @Override
        protected void cascadePre(Relation.Cascade cascadeType, ClientSession connection, OperationContext operationContext) {

        }

        @Override
        protected void cascadePost(Relation.Cascade cascadeType, ClientSession clientSession, OperationContext operationContext) {

        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {

        }

        @Override
        protected void executeUpdate(OpContext<ClientSession, Object> context, ClientSession session, DBOperation2<Integer, Integer, RuntimeException> fn) throws RuntimeException {
        }

        @Override
        protected void executeUpdate(OpContext<ClientSession, Object> context, ClientSession connection) throws RuntimeException {

        }

        @Override
        protected void veto(Predicate<T> predicate) {
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

        public T getEntity() {
            return entity;
        }
    }
}
