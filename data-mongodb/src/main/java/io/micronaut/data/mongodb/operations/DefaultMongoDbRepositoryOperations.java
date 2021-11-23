package io.micronaut.data.mongodb.operations;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.operations.internal.AbstractRepositoryOperations;
import io.micronaut.http.codec.MediaTypeCodec;

import java.util.List;

public class DefaultMongoDbRepositoryOperations extends AbstractRepositoryOperations<MongoCollection, ClientSession, RuntimeException> {

    private MongoClient mongoClient;

    /**
     * Default constructor.
     *
     * @param codecs                     The media type codecs
     * @param dateTimeProvider           The date time provider
     * @param runtimeEntityRegistry      The entity registry
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     */
    protected DefaultMongoDbRepositoryOperations(List<MediaTypeCodec> codecs,
                                                 DateTimeProvider<Object> dateTimeProvider,
                                                 RuntimeEntityRegistry runtimeEntityRegistry,
                                                 DataConversionService<?> conversionService,
                                                 AttributeConverterRegistry attributeConverterRegistry) {
        super(codecs, dateTimeProvider, runtimeEntityRegistry, conversionService, attributeConverterRegistry);
    }

    @Override
    protected AutoCloseable autoCloseable(ClientSession clientSession) {
        return clientSession;
    }

    @Override
    protected <T> String resolveAssociationInsert(Class repositoryType, RuntimePersistentEntity<T> persistentEntity, RuntimeAssociation<T> association) {
        return null;
    }

    @Override
    protected ConversionContext createTypeConversionContext(MongoCollection connection, RuntimePersistentProperty<?> property, Argument<?> argument) {
        return null;
    }

    @Override
    public void setStatementParameter(ClientSession preparedStatement, int index, DataType dataType, Object value, Dialect dialect) {
        // No op
    }
}
