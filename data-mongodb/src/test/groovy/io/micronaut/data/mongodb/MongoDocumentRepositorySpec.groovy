package io.micronaut.data.mongodb

import groovy.transform.Memoized
import io.micronaut.data.document.tck.AbstractDocumentRepositorySpec
import io.micronaut.data.document.tck.repositories.BasicTypesRepository

class MongoDocumentRepositorySpec extends AbstractDocumentRepositorySpec implements MongoDbTestPropertyProvider {

    @Memoized
    @Override
    BasicTypesRepository getBasicTypeRepository() {
        return context.getBean(MongoBasicTypesRepository)
    }

}
