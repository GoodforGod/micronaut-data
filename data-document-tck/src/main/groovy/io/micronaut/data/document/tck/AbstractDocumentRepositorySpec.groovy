package io.micronaut.data.document.tck

import io.micronaut.context.ApplicationContext
import io.micronaut.data.document.tck.entities.BasicTypes
import io.micronaut.data.document.tck.repositories.BasicTypesRepository
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractDocumentRepositorySpec extends Specification {

    abstract BasicTypesRepository getBasicTypeRepository()

    abstract Map<String, String> getProperties()

    @AutoCleanup
    @Shared
    ApplicationContext context = ApplicationContext.run(properties)

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

}
