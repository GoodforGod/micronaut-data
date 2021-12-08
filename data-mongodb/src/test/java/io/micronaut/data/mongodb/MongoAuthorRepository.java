package io.micronaut.data.mongodb;

import io.micronaut.data.document.tck.repositories.AuthorRepository;
import io.micronaut.data.mongodb.annotation.MongoDbRepository;

@MongoDbRepository
public interface MongoAuthorRepository extends AuthorRepository {
}
