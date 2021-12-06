package io.micronaut.data.mongodb;

import io.micronaut.data.mongodb.annotation.MongoDbRepository;
import io.micronaut.data.tck.repositories.AuthorRepository;

@MongoDbRepository
public interface MongoDbAuthorRepository extends AuthorRepository {
}
