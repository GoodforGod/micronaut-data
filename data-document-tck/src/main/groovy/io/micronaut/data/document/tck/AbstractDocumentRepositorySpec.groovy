package io.micronaut.data.document.tck

import io.micronaut.context.ApplicationContext
import io.micronaut.data.document.tck.entities.AuthorBooksDto
import io.micronaut.data.document.tck.entities.BasicTypes
import io.micronaut.data.document.tck.entities.Book
import io.micronaut.data.document.tck.entities.BookDto
import io.micronaut.data.document.tck.entities.Person
import io.micronaut.data.document.tck.repositories.BasicTypesRepository
import io.micronaut.data.document.tck.repositories.BookRepository
import io.micronaut.data.document.tck.repositories.PersonRepository
import io.micronaut.data.model.Pageable
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractDocumentRepositorySpec extends Specification {

    abstract BasicTypesRepository getBasicTypeRepository()

    abstract PersonRepository getPersonRepository()

    abstract BookRepository getBookRepository()

    abstract Map<String, String> getProperties()

    @AutoCleanup
    @Shared
    ApplicationContext context = ApplicationContext.run(properties)

    protected void setupBooks() {
        // book without an author
        bookRepository.save(new Book(title: "Anonymous", totalPages: 400))

        // blank title
        bookRepository.save(new Book(title: "", totalPages: 0))

        saveSampleBooks()
    }

    protected void saveSampleBooks() {
        bookRepository.saveAuthorBooks([
                new AuthorBooksDto("Stephen King", Arrays.asList(
                        new BookDto("The Stand", 1000),
                        new BookDto("Pet Cemetery", 400)
                )),
                new AuthorBooksDto("James Patterson", Arrays.asList(
                        new BookDto("Along Came a Spider", 300),
                        new BookDto("Double Cross", 300)
                )),
                new AuthorBooksDto("Don Winslow", Arrays.asList(
                        new BookDto("The Power of the Dog", 600),
                        new BookDto("The Border", 700)
                ))])
    }

    protected void savePersons(List<String> names) {
        personRepository.saveAll(names.collect { new Person(name: it) })
    }

    protected void setup() {
        cleanupData()
    }

    protected void cleanupData() {
        personRepository.deleteAll()
    }

    void "test save and retrieve basic types"() {
        when: "we save a new saved"
            def saved = basicTypeRepository.save(new BasicTypes())

        then: "The ID is assigned"
            saved.myId != null

        when: "A saved is found"
            def retrieved = basicTypeRepository.findById(saved.myId).orElse(null)

        then: "The saved is correct"
            retrieved.uuid == saved.uuid
            retrieved.bigDecimal == saved.bigDecimal
            retrieved.byteArray == saved.byteArray
            retrieved.charSequence == saved.charSequence
            retrieved.charset == saved.charset
            retrieved.primitiveBoolean == saved.primitiveBoolean
            retrieved.primitiveByte == saved.primitiveByte
            retrieved.primitiveChar == saved.primitiveChar
            retrieved.primitiveDouble == saved.primitiveDouble
            retrieved.primitiveFloat == saved.primitiveFloat
            retrieved.primitiveInteger == saved.primitiveInteger
            retrieved.primitiveLong == saved.primitiveLong
            retrieved.primitiveShort == saved.primitiveShort
            retrieved.wrapperBoolean == saved.wrapperBoolean
            retrieved.wrapperByte == saved.wrapperByte
            retrieved.wrapperChar == saved.wrapperChar
            retrieved.wrapperDouble == saved.wrapperDouble
            retrieved.wrapperFloat == saved.wrapperFloat
            retrieved.wrapperInteger == saved.wrapperInteger
            retrieved.wrapperLong == saved.wrapperLong
            retrieved.uri == saved.uri
            retrieved.url == saved.url
//            retrieved.instant == saved.instant
//            retrieved.localDateTime == saved.localDateTime
//            retrieved.zonedDateTime == saved.zonedDateTime
//            retrieved.offsetDateTime == saved.offsetDateTime
//            retrieved.dateCreated == saved.dateCreated
//            retrieved.dateUpdated == saved.dateUpdated
            retrieved.date == saved.date
    }

    void "test save one"() {
        given:
            savePersons(["Jeff", "James"])

        when:"one is saved"
            def person = new Person(name: "Fred")
            personRepository.save(person)

        then:"the instance is persisted"
            person.id != null
            personRepository.findById(person.id).isPresent()
            personRepository.get(person.id).name == 'Fred'
            personRepository.existsById(person.id)
            personRepository.count() == 3
            personRepository.count("Fred") == 1
            personRepository.findAll().size() == 3
    }

    void "test save many"() {
        given:
            savePersons(["Jeff", "James"])

        when:"many are saved"
            def p1 = personRepository.save("Frank", 0)
            def p2 = personRepository.save("Bob", 0)
            def people = [p1,p2]

        then:"all are saved"
            people.every { it.id != null }
            people.every { personRepository.findById(it.id).isPresent() }
            personRepository.findAll().size() == 4
            personRepository.count() == 4
            personRepository.count("Jeff") == 1

            personRepository.list(Pageable.from(1)).isEmpty()
            personRepository.list(Pageable.from(0, 1)).size() == 1
    }

    void "test update many"() {
        given:
            savePersons(["Jeff", "James"])

        when:
            def people = personRepository.findAll().toList()
            people.forEach() { it.name = it.name + " updated" }
            personRepository.updateAll(people)
            people = personRepository.findAll().toList()

        then:
            people.get(0).name.endsWith(" updated")
            people.get(1).name.endsWith(" updated")

        when:
            people = personRepository.findAll().toList()
            people.forEach() { it.name = it.name + " X" }
            def peopleUpdated = personRepository.updatePeople(people)
            people = personRepository.findAll().toList()

        then:
            peopleUpdated.size() == 2
            people.get(0).name.endsWith(" X")
            people.get(1).name.endsWith(" X")
            peopleUpdated.get(0).name.endsWith(" X")
            peopleUpdated.get(1).name.endsWith(" X")
    }

    void "test delete by id"() {
        given:
            savePersons(["Jeff", "James"])

        when:"an entity is retrieved"
            def person = personRepository.findByName("Jeff")

        then:"the person is not null"
            person != null
            person.name == 'Jeff'
            personRepository.findById(person.id).isPresent()

        when:"the person is deleted"
            personRepository.deleteById(person.id)

        then:"They are really deleted"
            !personRepository.findById(person.id).isPresent()
            old(personRepository.count()) - 1 == personRepository.count()
    }

    void "test delete by id and author id"() {
        given:
            setupBooks()
            def book = bookRepository.findByTitle("Pet Cemetery")
        when:
            int deleted = bookRepository.deleteByIdAndAuthorId(book.id, book.author.id)
        then:
            deleted == 1
            !bookRepository.findById(book.id).isPresent()
    }

    void "test delete by multiple ids"() {
        given:
            savePersons(["Jeff", "James"])

        when:"A search for some people"
            def people = personRepository.findByNameLike("J%")

        then:
            people.size() == 2

        when:"the people are deleted"
            personRepository.deleteAll(people)

        then:"Only the correct people are deleted"
            old(personRepository.count()) - 2 == personRepository.count()
            people.every { !personRepository.findById(it.id).isPresent() }
    }

    void "test delete one"() {
        given:
            savePersons(["Bob"])

        when:"A specific person is found and deleted"
            def bob = personRepository.findByName("Bob")

        then:"The person is present"
            bob != null

        when:"The person is deleted"
            personRepository.delete(bob)

        then:"They are deleted"
            !personRepository.findById(bob.id).isPresent()
            old(personRepository.count()) - 1 == personRepository.count()
    }

    void "test update one"() {
        given:
            savePersons(["Jeff", "James"])

        when:"A person is retrieved"
            def fred = personRepository.findByName("Jeff")

        then:"The person is present"
            fred != null

        when:"The person is updated"
            personRepository.updatePerson(fred.id, "Jack")

        then:"the person is updated"
            personRepository.findByName("Jeff") == null
            personRepository.findByName("Jack") != null

        when:"an update is issued that returns a number"
            def updated = personRepository.updateByName("Jack", 20)

        then:"The result is correct"
            updated == 1
            personRepository.findByName("Jack").age == 20

        when:"A whole entity is updated"
            def jack = personRepository.findByName("Jack")
            jack.setName("Jeffrey")
            jack.setAge(30)
            personRepository.update(jack)

        then:
            personRepository.findByName("Jack") == null
            personRepository.findByName("Jeffrey").age == 30
    }

    void "test delete all"() {
        given:
            int personsWithG = personRepository.findByNameLike("G%").size()

        when:"A new person is saved"
            personRepository.save("Greg", 30)
            personRepository.save("Groot", 300)

        then:"The count is "
            old(personRepository.count()) + 2 == personRepository.count()

        when:"batch delete occurs"
            def deleted = personRepository.deleteByNameLike("G%")

        then:"The count is back to 1 and it entries were deleted"
            deleted == personsWithG + 2
            old(personRepository.count()) - (personsWithG + 2) == personRepository.count()

        when:"everything is deleted"
            personRepository.deleteAll()

        then:"data is gone"
            personRepository.count() == 0
    }

}
