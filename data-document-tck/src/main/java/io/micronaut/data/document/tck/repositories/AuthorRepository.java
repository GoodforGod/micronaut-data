package io.micronaut.data.document.tck.repositories;

import io.micronaut.data.document.tck.entities.Author;
import io.micronaut.data.repository.CrudRepository;

public interface AuthorRepository extends CrudRepository<Author, String> {
}
