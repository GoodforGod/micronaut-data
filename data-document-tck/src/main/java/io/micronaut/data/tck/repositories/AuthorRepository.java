package io.micronaut.data.tck.repositories;

import io.micronaut.data.repository.CrudRepository;
import io.micronaut.data.tck.entities.Author;

public interface AuthorRepository extends CrudRepository<Author, String> {
}
