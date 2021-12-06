package io.micronaut.data.document.processor.mapper;

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.document.serde.OneRelationDeserializer;
import io.micronaut.data.document.serde.OneRelationSerializer;
import io.micronaut.inject.annotation.TypedAnnotationMapper;
import io.micronaut.inject.visitor.VisitorContext;
import io.micronaut.serde.config.annotation.SerdeConfig;

import java.util.Collections;
import java.util.List;

public class MappedRelationMapper implements TypedAnnotationMapper<Relation> {

    @Override
    public Class<Relation> annotationType() {
        return Relation.class;
    }

    @Override
    public List<AnnotationValue<?>> map(AnnotationValue<Relation> annotation, VisitorContext visitorContext) {
        Relation.Kind kind = annotation.getRequiredValue(Relation.Kind.class);
        if (kind == Relation.Kind.MANY_TO_ONE) {
            return Collections.singletonList(
                    AnnotationValue.builder(SerdeConfig.class)
                            .member(SerdeConfig.SERIALIZER_CLASS, OneRelationSerializer.class)
                            .member(SerdeConfig.DESERIALIZER_CLASS, OneRelationDeserializer.class)
                            .build()
            );
        } else if (kind != Relation.Kind.EMBEDDED) {
            return Collections.singletonList(
                    AnnotationValue.builder(SerdeConfig.class)
                            .member(SerdeConfig.IGNORED, true)
                            .build()
            );
        }
        return Collections.emptyList();
    }

}
