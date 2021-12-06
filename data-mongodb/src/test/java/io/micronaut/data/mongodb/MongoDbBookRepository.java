package io.micronaut.data.mongodb;

import io.micronaut.data.mongodb.annotation.MongoDbRepository;
import io.micronaut.data.tck.repositories.AuthorRepository;
import io.micronaut.data.tck.repositories.BookRepository;

@MongoDbRepository
public abstract class MongoDbBookRepository extends BookRepository {

    public MongoDbBookRepository(AuthorRepository authorRepository) {
        super(authorRepository);
    }

}
