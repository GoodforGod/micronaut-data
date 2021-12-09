package io.micronaut.data.document.mongodb.repositories;

import io.micronaut.data.document.tck.repositories.BasicTypesRepository;
import io.micronaut.data.document.mongodb.annotation.MongoDbRepository;

@MongoDbRepository
public interface MongoBasicTypesRepository extends BasicTypesRepository {
}
