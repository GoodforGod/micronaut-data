package io.micronaut.data.mongodb;

import io.micronaut.data.document.tck.repositories.PersonRepository;
import io.micronaut.data.mongodb.annotation.MongoDbRepository;

@MongoDbRepository
public interface MongoPersonRepository extends PersonRepository {
}
