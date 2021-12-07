package io.micronaut.data.mongodb;

import io.micronaut.data.document.tck.repositories.AuthorRepository;
import io.micronaut.data.document.tck.repositories.BookRepository;
import io.micronaut.data.mongodb.annotation.MongoDbRepository;

@MongoDbRepository
public abstract class MongoDbBookRepository extends BookRepository {

    public MongoDbBookRepository(AuthorRepository authorRepository) {
        super(authorRepository);
    }

}
