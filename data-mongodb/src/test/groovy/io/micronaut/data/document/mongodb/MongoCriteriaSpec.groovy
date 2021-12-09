/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.data.document.mongodb

import groovy.transform.CompileStatic
import io.micronaut.context.ApplicationContext
import io.micronaut.core.annotation.NonNull
import io.micronaut.data.document.model.query.builder.MongoDbQueryBuilder
import io.micronaut.data.document.mongodb.entities.Test
import io.micronaut.data.event.EntityEventListener
import io.micronaut.data.model.jpa.criteria.PersistentEntityCriteriaBuilder
import io.micronaut.data.model.jpa.criteria.PersistentEntityCriteriaDelete
import io.micronaut.data.model.jpa.criteria.PersistentEntityCriteriaQuery
import io.micronaut.data.model.jpa.criteria.PersistentEntityCriteriaUpdate
import io.micronaut.data.model.jpa.criteria.PersistentEntityRoot
import io.micronaut.data.model.jpa.criteria.impl.QueryResultPersistentEntityCriteriaQuery
import io.micronaut.data.model.runtime.RuntimeEntityRegistry
import io.micronaut.data.model.runtime.RuntimePersistentEntity
import io.micronaut.data.model.runtime.RuntimePersistentProperty
import io.micronaut.data.runtime.criteria.RuntimeCriteriaBuilder
import jakarta.persistence.criteria.CriteriaBuilder
import jakarta.persistence.criteria.CriteriaDelete
import jakarta.persistence.criteria.CriteriaQuery
import jakarta.persistence.criteria.CriteriaUpdate
import jakarta.persistence.criteria.Predicate
import jakarta.persistence.criteria.Root
import spock.lang.Specification
import spock.lang.Unroll

class MongoCriteriaSpec extends Specification {

    PersistentEntityCriteriaBuilder criteriaBuilder

    PersistentEntityCriteriaQuery criteriaQuery

    PersistentEntityCriteriaDelete criteriaDelete

    PersistentEntityCriteriaUpdate criteriaUpdate

    void setup() {
        Map<Class, RuntimePersistentEntity> map = new HashMap<>();
        criteriaBuilder = new RuntimeCriteriaBuilder(new RuntimeEntityRegistry() {
            @Override
            EntityEventListener<Object> getEntityEventListener() {
                throw new IllegalStateException()
            }

            @Override
            Object autoPopulateRuntimeProperty(RuntimePersistentProperty<?> persistentProperty, Object previousValue) {
                throw new IllegalStateException()
            }

            @Override
            <T> RuntimePersistentEntity<T> getEntity(Class<T> type) {
                return map.computeIfAbsent(type, RuntimePersistentEntity::new)
            }

            @Override
            <T> RuntimePersistentEntity<T> newEntity(Class<T> type) {
                throw new IllegalStateException()
            }

            @Override
            ApplicationContext getApplicationContext() {
                throw new IllegalStateException()
            }
        })
        criteriaQuery = criteriaBuilder.createQuery()
        criteriaDelete = criteriaBuilder.createCriteriaDelete(Test)
        criteriaUpdate = criteriaBuilder.createCriteriaUpdate(Test)
    }

    PersistentEntityRoot createRoot(CriteriaQuery query) {
        return query.from(Test)
    }

    PersistentEntityRoot createRoot(CriteriaDelete query) {
        return query.from(Test)
    }

    PersistentEntityRoot createRoot(CriteriaUpdate query) {
        return query.from(Test)
    }

    @Unroll
    void "test criteria predicate"(Specification specification) {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            def predicate = specification.toPredicate(entityRoot, criteriaQuery, criteriaBuilder)
            if (predicate) {
                criteriaQuery.where(predicate)
            }
            String whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            specification << [
                    { root, query, cb ->
                        cb.between(root.get("enabled"), true, false)
                    } as Specification,
                    { root, query, cb ->
                        def parameter = cb.parameter(Integer)
                        cb.between(root.get("amount"), parameter, parameter)
                    } as Specification,
                    { root, query, cb ->
                        query.where(root.get("enabled"))
                        null
                    } as Specification,
                    { root, query, cb ->
                        query.where(root.get("enabled"))
                        query.orderBy(cb.desc(root.get("amount")), cb.asc(root.get("budget")))
                        null
                    } as Specification,
                    { root, query, cb ->
                        cb.isTrue(root.get("enabled"))
                    } as Specification,
                    { root, query, cb ->
                        cb.and(cb.isTrue(root.get("enabled")), cb.isTrue(root.get("enabled")))
                    } as Specification
            ]
            expectedWhereQuery << [
                    '{ $and: [ { enabled: { $gte: true } }, { enabled: { $lte: false } } ] }',
                    '{ $and: [ { amount: { $gte: { $qpidx: 0 } } }, { amount: { $lte: { $qpidx: 1 } } } ] }',
                    '{ enabled: { $eq: true } }',
                    '{ enabled: { $eq: true } }',
                    '{ enabled: { $eq: true } }',
                    '{ $and: [ { enabled: { $eq: true } }, { enabled: { $eq: true } } ] }',
            ]
    }

    @Unroll
    void "test #predicate predicate produces where query: #expectedWhereQuery"() {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            criteriaQuery.where(criteriaBuilder."$predicate"(entityRoot.get(property)))
            def whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            property   | predicate   | expectedWhereQuery
            "enabled"  | "isTrue"    | '{ enabled: { $eq: true } }'
            "enabled2" | "isTrue"    | '{ enabled2: { $eq: true } }'
            "enabled"  | "isFalse"   | '{ enabled: { $eq: false } }'
            "enabled2" | "isFalse"   | '{ enabled2: { $eq: false } }'
            "enabled"  | "isNull"    | '{ enabled: { $eq: null } }'
            "enabled2" | "isNull"    | '{ enabled2: { $eq: null } }'
            "enabled"  | "isNotNull" | '{ enabled: { $ne: null } }'
            "enabled2" | "isNotNull" | '{ enabled2: { $ne: null } }'
            "name"     | "isNotNull" | '{ name: { $ne: null } }'
    }

    @Unroll
    void "test not #predicate predicate produces where query: #expectedWhereQuery"() {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            criteriaQuery.where(criteriaBuilder."$predicate"(entityRoot.get(property)).not())
            def whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            property   | predicate   | expectedWhereQuery
            "enabled"  | "isTrue"    | '{ enabled: { $eq: false } }'
            "enabled2" | "isTrue"    | '{ enabled2: { $eq: false } }'
            "enabled"  | "isFalse"   | '{ enabled: { $eq: true } }'
            "enabled2" | "isFalse"   | '{ enabled2: { $eq: true } }'
            "enabled"  | "isNull"    | '{ enabled: { $ne: null } }'
            "enabled2" | "isNull"    | '{ enabled2: { $ne: null } }'
            "enabled"  | "isNotNull" | '{ enabled: { $eq: null } }'
            "enabled2" | "isNotNull" | '{ enabled2: { $eq: null } }'
            "name"     | "isNotNull" | '{ name: { $eq: null } }'
    }

    @Unroll
    void "test properties #predicate predicate produces where query: #expectedWhereQuery"() {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            criteriaQuery.where(criteriaBuilder."$predicate"(entityRoot.get(property1), entityRoot.get(property2)))
            def whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            property1 | property2  | predicate              | expectedWhereQuery
            "enabled" | "enabled2" | "equal"                | '{ $expr: { $eq: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "notEqual"             | '{ $expr: { $ne: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "greaterThan"          | '{ $expr: { $gt: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "greaterThanOrEqualTo" | '{ $expr: { $gte: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "lessThan"             | '{ $expr: { $lt: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "lessThanOrEqualTo"    | '{ $expr: { $lte: ["$enabled", "$enabled2"] } }'
            "amount"  | "budget"   | "gt"                   | '{ $expr: { $gt: ["$amount", "$budget"] } }'
            "amount"  | "budget"   | "ge"                   | '{ $expr: { $gte: ["$amount", "$budget"] } }'
            "amount"  | "budget"   | "lt"                   | '{ $expr: { $lt: ["$amount", "$budget"] } }'
            "amount"  | "budget"   | "le"                   | '{ $expr: { $lte: ["$amount", "$budget"] } }'
    }

    @Unroll
    void "test properties not #predicate predicate produces where query: #expectedWhereQuery"() {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            criteriaQuery.where(criteriaBuilder."$predicate"(entityRoot.get(property1), entityRoot.get(property2)).not())
            def whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            property1 | property2  | predicate              | expectedWhereQuery
            "enabled" | "enabled2" | "equal"                | '{ $expr: { $ne: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "notEqual"             | '{ $expr: { $eq: ["$enabled", "$enabled2"] } }'
            "enabled" | "enabled2" | "greaterThan"          | '{ $not: { $expr: { $gt: ["$enabled", "$enabled2"] } } }'
            "enabled" | "enabled2" | "greaterThanOrEqualTo" | '{ $not: { $expr: { $gte: ["$enabled", "$enabled2"] } } }'
            "enabled" | "enabled2" | "lessThan"             | '{ $not: { $expr: { $lt: ["$enabled", "$enabled2"] } } }'
            "enabled" | "enabled2" | "lessThanOrEqualTo"    | '{ $not: { $expr: { $lte: ["$enabled", "$enabled2"] } } }'
            "amount"  | "budget"   | "gt"                   | '{ $not: { $expr: { $gt: ["$amount", "$budget"] } } }'
            "amount"  | "budget"   | "ge"                   | '{ $not: { $expr: { $gte: ["$amount", "$budget"] } } }'
            "amount"  | "budget"   | "lt"                   | '{ $not: { $expr: { $lt: ["$amount", "$budget"] } } }'
            "amount"  | "budget"   | "le"                   | '{ $not: { $expr: { $lte: ["$amount", "$budget"] } } }'
    }

    @Unroll
    void "test property value #predicate predicate produces where query: #expectedWhereQuery"() {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            criteriaQuery.where(criteriaBuilder."$predicate"(entityRoot.get(property1), value))
            def whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            property1 | value                   | predicate              | expectedWhereQuery
            "enabled" | true                    | "equal"                | '{ enabled: { $eq: true } }'
            "enabled" | true                    | "notEqual"             | '{ enabled: { $ne: true } }'
            "enabled" | true                    | "greaterThan"          | '{ enabled: { $gt: true } }'
            "enabled" | true                    | "greaterThanOrEqualTo" | '{ enabled: { $gte: true } }'
            "enabled" | true                    | "lessThan"             | '{ enabled: { $lt: true } }'
            "enabled" | true                    | "lessThanOrEqualTo"    | '{ enabled: { $lte: true } }'
            "amount"  | BigDecimal.valueOf(100) | "gt"                   | '{ amount: { $gt: 100 } }'
            "amount"  | BigDecimal.valueOf(100) | "ge"                   | '{ amount: { $gte: 100 } }'
            "amount"  | BigDecimal.valueOf(100) | "lt"                   | '{ amount: { $lt: 100 } }'
            "amount"  | BigDecimal.valueOf(100) | "le"                   | '{ amount: { $lte: 100 } }'
    }

    @Unroll
    void "test property value not #predicate predicate produces where query: #expectedWhereQuery"() {
        given:
            PersistentEntityRoot entityRoot = createRoot(criteriaQuery)
            criteriaQuery.where(criteriaBuilder."$predicate"(entityRoot.get(property1), value).not())
            def whereSqlQuery = getWhereQueryPart(criteriaQuery)

        expect:
            whereSqlQuery == expectedWhereQuery

        where:
            property1 | value                   | predicate              | expectedWhereQuery
            "enabled" | true                    | "equal"                | '{ enabled: { $ne: true } }'
            "enabled" | true                    | "notEqual"             | '{ enabled: { $eq: true } }'
            "enabled" | true                    | "greaterThan"          | '{ $not: { enabled: { $gt: true } } }'
            "enabled" | true                    | "greaterThanOrEqualTo" | '{ $not: { enabled: { $gte: true } } }'
            "enabled" | true                    | "lessThan"             | '{ $not: { enabled: { $lt: true } } }'
            "enabled" | true                    | "lessThanOrEqualTo"    | '{ $not: { enabled: { $lte: true } } }'
            "amount"  | BigDecimal.valueOf(100) | "gt"                   | '{ $not: { amount: { $gt: 100 } } }'
            "amount"  | BigDecimal.valueOf(100) | "ge"                   | '{ $not: { amount: { $gte: 100 } } }'
            "amount"  | BigDecimal.valueOf(100) | "lt"                   | '{ $not: { amount: { $lt: 100 } } }'
            "amount"  | BigDecimal.valueOf(100) | "le"                   | '{ $not: { amount: { $lte: 100 } } }'
    }

    private static String getSelectQueryPart(PersistentEntityCriteriaQuery<Object> query) {
        def sqlQuery = getSqlQuery(query)
        return sqlQuery.substring("SELECT ".length(), sqlQuery.indexOf(" FROM"))
    }

    private static String getWhereQueryPart(PersistentEntityCriteriaQuery<Object> query) {
        def sqlQuery = getSqlQuery(query)
        return sqlQuery
    }

    private static String getSqlQuery(def query) {
        return ((QueryResultPersistentEntityCriteriaQuery) query).buildQuery(new MongoDbQueryBuilder()).getQuery()
    }

    @CompileStatic
    interface Specification<T> {
        Predicate toPredicate(@NonNull Root<T> root, @NonNull CriteriaQuery<?> query, @NonNull CriteriaBuilder criteriaBuilder);
    }

    @CompileStatic
    interface DeleteSpecification<T> {
        Predicate toPredicate(@NonNull Root<T> root, @NonNull CriteriaDelete<?> query, @NonNull CriteriaBuilder criteriaBuilder);
    }

    @CompileStatic
    interface UpdateSpecification<T> {
        Predicate toPredicate(@NonNull Root<T> root, @NonNull CriteriaUpdate<?> query, @NonNull CriteriaBuilder criteriaBuilder);
    }

}
