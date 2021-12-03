package io.micronaut.data.document.processor.mapper;

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.document.serde.IdSerializer;
import io.micronaut.inject.annotation.TypedAnnotationMapper;
import io.micronaut.inject.visitor.VisitorContext;
import io.micronaut.serde.config.annotation.SerdeConfig;

import java.util.Collections;
import java.util.List;

public class MappedIdMapper implements TypedAnnotationMapper<Id> {

    @Override
    public Class<Id> annotationType() {
        return Id.class;
    }

    @Override
    public List<AnnotationValue<?>> map(AnnotationValue<Id> annotation, VisitorContext visitorContext) {
        return Collections.singletonList(
                AnnotationValue.builder(SerdeConfig.class).member(SerdeConfig.SERIALIZER_CLASS, IdSerializer.class.getName()).build()
        );
    }

}
