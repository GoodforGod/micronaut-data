package io.micronaut.data.mongodb.operations;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
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
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.http.codec.MediaTypeCodec;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
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
        RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
        MongoCollection<BsonDocument> collection = mongoClient.getDatabase("default").getCollection("authors", BsonDocument.class);
        BsonDocument a = new BsonDocument();
        a.put("name", new BsonString("Hello"));
        InsertOneResult one = collection.insertOne(a);
        BsonDocument bsonDocument = collection.find(BsonDocument.class).first();
        return null;
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
        return null;
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
}
