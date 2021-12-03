package io.micronaut.data.mongodb.operations;

import com.mongodb.client.model.Filters;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Date;

@Internal
final class Utils {

    static Bson filterById(ConversionService<?> conversionService, RuntimePersistentEntity<?> persistentEntity, Object value) {
        RuntimePersistentProperty<?> identity = persistentEntity.getIdentity();
        if (identity != null) {
            AnnotationValue<BsonRepresentation> bsonRepresentation = identity.getAnnotationMetadata().getAnnotation(BsonRepresentation.class);
            if (bsonRepresentation != null) {
                BsonType bsonType = bsonRepresentation.getRequiredValue(BsonType.class);
                return Filters.eq(toBsonValue(conversionService, bsonType, value));
            } else {
                Class<?> type = identity.getProperty().getType();
                if (type == String.class) {
                    return Filters.eq(new ObjectId(value.toString()));
                }
            }
        }
        throw new IllegalStateException("Cannot determine id!");
    }

    static BsonValue toBsonValue(ConversionService<?> conversionService, BsonType bsonType, Object value) {
        switch (bsonType) {
            case STRING:
                return new BsonString(value.toString());
            case OBJECT_ID:
                if (value instanceof String) {
                    return new BsonObjectId(new ObjectId((String) value));
                }
                if (value instanceof byte[]) {
                    return new BsonObjectId(new ObjectId((byte[]) value));
                }
                if (value instanceof Date) {
                    return new BsonObjectId(new ObjectId((Date) value));
                }
                return new BsonObjectId(conversionService.convertRequired(value, ObjectId.class));
            default:
                throw new IllegalStateException("Bson conversion to: " + bsonType + " is missing!");
        }
    }


}
