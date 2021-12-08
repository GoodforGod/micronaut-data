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
import io.micronaut.data.document.tck.entities.AuthorBooksDto
import io.micronaut.data.document.tck.entities.Book
import io.micronaut.data.document.tck.entities.BookDto
import io.micronaut.data.document.tck.entities.Customer
import io.micronaut.data.model.runtime.InsertOperation
import io.micronaut.data.model.runtime.RuntimeEntityRegistry
import io.micronaut.data.model.runtime.StoredQuery
import io.micronaut.data.model.runtime.UpdateOperation
import io.micronaut.data.mongodb.operations.DefaultMongoDbRepositoryOperations
import io.micronaut.data.mongodb.operations.DefaultReactiveMongoDbRepositoryOperations
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

    @Inject
    DefaultReactiveMongoDbRepositoryOperations reactiveMongoDbRepositoryOperations

    RuntimeEntityRegistry runtimeEntityRegistry

    @Inject
    MongoClient mongoClient

    @Inject
    MongoBookRepository bookRepository

    @Inject
    MongoAuthorRepository authorRepository

    protected void setupBooks() {
        // book without an author
        bookRepository.save(new Book(title: "Anonymous", totalPages: 400))

        // blank title
        bookRepository.save(new Book(title: "", totalPages: 0))

        saveSampleBooks()
    }

    protected void saveSampleBooks() {
        bookRepository.saveAuthorBooks([
                new AuthorBooksDto("Stephen King", Arrays.asList(
                        new BookDto("The Stand", 1000),
                        new BookDto("Pet Cemetery", 400)
                )),
                new AuthorBooksDto("James Patterson", Arrays.asList(
                        new BookDto("Along Came a Spider", 300),
                        new BookDto("Double Cross", 300)
                )),
                new AuthorBooksDto("Don Winslow", Arrays.asList(
                        new BookDto("The Power of the Dog", 600),
                        new BookDto("The Border", 700)
                ))])
    }

    void "add all"() {
        when:
            setupBooks()

            def list2 = bookRepository.queryAll().toList()
            def list = bookRepository.findAll().toList()
        then:
            list2.size() == 8
    }

    void "test insert"() {
        when:
            def persisted = dbRepositoryOperations.persist(insertOperation(new Customer(firstName: "John", lastName: "Doe")))
        then:
            persisted.id
        when:
            def found = dbRepositoryOperations.findOne(Customer, persisted.id)
        then:
            found.id == persisted.id
            found.firstName == persisted.firstName
            found.lastName == persisted.lastName
        when:
            found.firstName = "Denis"
            dbRepositoryOperations.update(updateOperation(found))
            def updated = dbRepositoryOperations.findOne(Customer, persisted.id)
        then:
            updated.id == persisted.id
            updated.firstName == "Denis"
            updated.lastName == "Doe"
    }

    void "test insert reactive"() {
        when:
            def persisted = reactiveMongoDbRepositoryOperations.persist(insertOperation(new Customer(firstName: "John", lastName: "Doe"))).block()
        then:
            persisted.id
        when:
            def found = reactiveMongoDbRepositoryOperations.findOne(Customer, persisted.id).block()
        then:
            found.id == persisted.id
            found.firstName == persisted.firstName
            found.lastName == persisted.lastName
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

    @CompileStatic
    <T> UpdateOperation<T> updateOperation(T instance) {
        return new UpdateOperation<T>() {
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
