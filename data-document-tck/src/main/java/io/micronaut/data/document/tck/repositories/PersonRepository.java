package io.micronaut.data.document.tck.repositories;

import io.micronaut.data.document.tck.entities.Person;
import io.micronaut.data.repository.CrudRepository;

public interface PersonRepository extends CrudRepository<Person, String> {

    Person get(String id);

}
