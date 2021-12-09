package io.micronaut.data.document.tck.repositories;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.document.tck.entities.Person;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;

public interface PersonRepository extends CrudRepository<Person, String> {

    Person get(String id);

    int count(String name);

    Person save(@Parameter("name") String name, @Parameter("age") int age);

    List<Person> list(Pageable pageable);

    long updateAll(List<Person> people);

    List<Person> updatePeople(List<Person> people);

    @Nullable
    Person findByName(String name);
}
