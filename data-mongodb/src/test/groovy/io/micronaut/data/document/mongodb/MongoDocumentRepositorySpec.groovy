package io.micronaut.data.document.mongodb

import groovy.transform.Memoized
import io.micronaut.data.document.mongodb.repositories.MongoBasicTypesRepository
import io.micronaut.data.document.mongodb.repositories.MongoPersonRepository
import io.micronaut.data.document.tck.AbstractDocumentRepositorySpec
import io.micronaut.data.document.tck.repositories.BasicTypesRepository
import io.micronaut.data.document.tck.repositories.PersonRepository

class MongoDocumentRepositorySpec extends AbstractDocumentRepositorySpec implements MongoDbTestPropertyProvider {

    @Memoized
    @Override
    BasicTypesRepository getBasicTypeRepository() {
        return context.getBean(MongoBasicTypesRepository)
    }

    @Memoized
    @Override
    PersonRepository getPersonRepository() {
        return context.getBean(MongoPersonRepository)
    }
}
