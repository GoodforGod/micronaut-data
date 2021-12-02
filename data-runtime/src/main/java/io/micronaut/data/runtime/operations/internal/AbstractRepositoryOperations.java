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
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.event.DefaultEntityEventContext;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.MediaTypeCodec;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    public static class OperationContext {
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
