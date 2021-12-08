package io.micronaut.data.mongodb

import groovy.transform.Memoized
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
