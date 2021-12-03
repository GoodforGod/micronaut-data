package io.micronaut.data.document.processor.mapper;

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.AnnotationValueBuilder;
import io.micronaut.data.annotation.MappedProperty;
import io.micronaut.inject.annotation.TypedAnnotationMapper;
import io.micronaut.inject.visitor.VisitorContext;
import io.micronaut.serde.config.annotation.SerdeConfig;

import java.util.Collections;
import java.util.List;

public class MappedPropertyMapper implements TypedAnnotationMapper<io.micronaut.data.annotation.MappedProperty> {

    @Override
    public Class<io.micronaut.data.annotation.MappedProperty> annotationType() {
        return io.micronaut.data.annotation.MappedProperty.class;
    }

    @Override
    public List<AnnotationValue<?>> map(AnnotationValue<MappedProperty> annotation, VisitorContext visitorContext) {
        AnnotationValueBuilder<SerdeConfig> builder = AnnotationValue.builder(SerdeConfig.class);
        annotation.stringValue().ifPresent(property -> builder.member(SerdeConfig.PROPERTY, property));
        return Collections.singletonList(builder.build());
    }

}
