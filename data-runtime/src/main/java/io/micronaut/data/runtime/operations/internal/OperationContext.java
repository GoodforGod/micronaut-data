package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.data.model.Association;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OperationContext {
    public final AnnotationMetadata annotationMetadata;
    public final Class<?> repositoryType;
    public final List<Association> associations = Collections.emptyList();
    public final Set<Object> persisted = new HashSet<>(5);

    public OperationContext(AnnotationMetadata annotationMetadata, Class<?> repositoryType) {
        this.annotationMetadata = annotationMetadata;
        this.repositoryType = repositoryType;
    }
}
