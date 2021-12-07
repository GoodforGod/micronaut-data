package io.micronaut.data.mongodb;

import io.micronaut.data.document.tck.repositories.BasicTypesRepository;
import io.micronaut.data.mongodb.annotation.MongoDbRepository;

@MongoDbRepository
public interface MongoBasicTypesRepository extends BasicTypesRepository {
}
