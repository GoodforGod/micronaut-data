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
package io.micronaut.data.mongodb

import com.mongodb.client.MongoClient
import groovy.transform.CompileStatic
import io.micronaut.core.convert.value.ConvertibleValues
import io.micronaut.core.type.Argument
import io.micronaut.data.model.runtime.InsertOperation
import io.micronaut.data.model.runtime.RuntimeEntityRegistry
import io.micronaut.data.model.runtime.StoredQuery
import io.micronaut.data.mongodb.operations.DefaultMongoDbRepositoryOperations
import io.micronaut.data.tck.entities.Country
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import org.bson.UuidRepresentation
import org.testcontainers.containers.MongoDBContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

@MicronautTest
class BasicSpec extends Specification implements TestPropertyProvider {

    @Shared @AutoCleanup MongoDBContainer mongo = new MongoDBContainer()

    @Inject
    DefaultMongoDbRepositoryOperations dbRepositoryOperations

    RuntimeEntityRegistry runtimeEntityRegistry

    @Inject
    MongoClient mongoClient

    void "test insert"() {
        when:
            def persisted = dbRepositoryOperations.persist(insertOperation(new Country("Czech Republic")))
        then:
            persisted.uuid
        when:
            def found = dbRepositoryOperations.findOne(Country, persisted.uuid)
        then:
            found.uuid == persisted.uuid
            found.name == persisted.name
    }

    @CompileStatic
    <T> InsertOperation<T> insertOperation(T instance) {
        return new InsertOperation<T>() {
            @Override
            T getEntity() {
                return instance
            }

            @Override
            Class<T> getRootEntity() {
                return instance.getClass() as Class<T>
            }

            @Override
            Class<?> getRepositoryType() {
                return Object.class
            }

            @Override
            StoredQuery<T, ?> getStoredQuery() {
                return null
            }

            @Override
            String getName() {
                return instance.getClass().name
            }

            @Override
            ConvertibleValues<Object> getAttributes() {
                return ConvertibleValues.EMPTY
            }

            @Override
            Argument<T> getResultArgument() {
                return Argument.of(instance.getClass()) as Argument<T>
            }
        }
    }

    @Override
    Map<String, String> getProperties() {
        mongo.start()
        return [
                'mongodb.uri': mongo.replicaSetUrl,
                'mongodb.uuid-representation': UuidRepresentation.STANDARD
        ]
    }
}
