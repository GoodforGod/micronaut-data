package io.micronaut.data.document.model.query.builder;

import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.data.annotation.TypeRole;
import io.micronaut.data.model.Association;
import io.micronaut.data.model.Embedded;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.PersistentEntity;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.Sort;
import io.micronaut.data.model.naming.NamingStrategy;
import io.micronaut.data.model.query.BindingParameter;
import io.micronaut.data.model.query.QueryModel;
import io.micronaut.data.model.query.builder.QueryBuilder;
import io.micronaut.data.model.query.builder.QueryParameterBinding;
import io.micronaut.data.model.query.builder.QueryResult;
import io.micronaut.serde.config.annotation.SerdeConfig;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class MongoDbQueryBuilder implements QueryBuilder {

    protected final Map<Class, CriterionHandler> queryHandlers = new HashMap<>(30);

    {
        addCriterionHandler(QueryModel.Negation.class, (ctx, sb, negation) -> {
            if (negation.getCriteria().size() == 1) {
                append(sb, "$not", sb2 -> handleJunction(ctx, sb2, negation, "$or"));
            } else {
                append(sb, "$not", sb2 -> handleCriterion(ctx, sb2, negation.getCriteria().iterator().next()));
            }
        });

        addCriterionHandler(QueryModel.Conjunction.class, (ctx, sb, conjunction) -> handleJunction(ctx, sb, conjunction, "$and"));

        addCriterionHandler(QueryModel.Disjunction.class, (ctx, sb, disjunction) -> {
            handleJunction(ctx, sb, disjunction, "$or");
        });

        addCriterionHandler(QueryModel.IsTrue.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals(criterion.getProperty(), true)));
        addCriterionHandler(QueryModel.IsFalse.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals(criterion.getProperty(), false)));
        addCriterionHandler(QueryModel.Equals.class, propertyOperatorExpression("$eq"));
        addCriterionHandler(QueryModel.IdEquals.class, (context, sb, criterion) ->  handleCriterion(context, sb, new QueryModel.Equals("id", criterion.getValue())));
        addCriterionHandler(QueryModel.NotEquals.class, propertyOperatorExpression("$ne"));
        addCriterionHandler(QueryModel.GreaterThan.class, propertyOperatorExpression("$gt"));
        addCriterionHandler(QueryModel.GreaterThanEquals.class, propertyOperatorExpression("$gte"));
        addCriterionHandler(QueryModel.LessThan.class, propertyOperatorExpression("$lt"));
        addCriterionHandler(QueryModel.LessThanEquals.class, propertyOperatorExpression("$lte"));
        addCriterionHandler(QueryModel.IsNull.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.Equals(criterion.getProperty(), null)));
        addCriterionHandler(QueryModel.IsNotNull.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.NotEquals(criterion.getProperty(), null)));
        addCriterionHandler(QueryModel.IsNotNull.class, (context, sb, criterion) -> handleCriterion(context, sb, new QueryModel.NotEquals(criterion.getProperty(), null)));
        addCriterionHandler(QueryModel.GreaterThanProperty.class, comparison("$gt"));
        addCriterionHandler(QueryModel.GreaterThanEqualsProperty.class, comparison("$gte"));
        addCriterionHandler(QueryModel.LessThanProperty.class, comparison("$lt"));
        addCriterionHandler(QueryModel.LessThanEqualsProperty.class, comparison("$lte"));
        addCriterionHandler(QueryModel.EqualsProperty.class, comparison("$eq"));
        addCriterionHandler(QueryModel.NotEqualsProperty.class, comparison("$ne"));
    }

    private <T extends QueryModel.PropertyCriterion> CriterionHandler<T> propertyOperatorExpression(String op) {
        return (context, sb, criterion) -> {
            Object value = criterion.getValue();
            QueryPropertyPath propertyPath = context.getRequiredProperty(criterion);
            String propertyName = propertyPath.getProperty().getAnnotationMetadata()
                    .stringValue(SerdeConfig.class, SerdeConfig.PROPERTY)
                    .orElse(propertyPath.getProperty().getName());
            System.out.println(context.getPersistentEntity().getName() + " " + propertyName + " " + propertyPath.getProperty().getAnnotationMetadata().getAnnotationNames());
            appendPropertyExp(sb, op, propertyName, () -> {
                if (value instanceof BindingParameter) {
                    context.pushParameter(
                            (BindingParameter) value,
                            newBindingContext(propertyPath.propertyPath)
                    );
                } else {
                    sb.append(asLiteral(value));
                }
            });
        };
    }

    private <T extends QueryModel.PropertyComparisonCriterion> CriterionHandler<T> comparison(String operator) {
        return (ctx, sb, comparisonCriterion) -> {
            QueryPropertyPath p1 = ctx.getRequiredProperty(comparisonCriterion.getProperty(), comparisonCriterion.getClass());
            QueryPropertyPath p2 = ctx.getRequiredProperty(comparisonCriterion.getOtherProperty(), comparisonCriterion.getClass());

            appendPropertyExp(sb, operator, "$expr", () -> {
                sb.append("[").append("\"$").append(p1.getPath()).append('"').append(", ").append("\"$").append(p2.getPath()).append('"').append("]");
            });
        };
    }

    protected String asLiteral(@Nullable Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number) {
            return Long.toString(((Number) value).longValue());
        }
        if (value instanceof Boolean) {
            return value.toString().toLowerCase(Locale.ROOT);
        }
        return "'" + value + "'";
    }


    private void append(StringBuilder sb, String property, Consumer<StringBuilder> consumer) {
        sb.append("{ ");
        sb.append(property).append(": ");
        consumer.accept(sb);
        sb.append(" }");
    }

    private void appendPropertyExp(StringBuilder sb, String operator, String property, Runnable placeholder) {
        sb.append("{ ");
        sb.append(property).append(": ");
        sb.append("{ ");
        sb.append(operator).append(": ");
        placeholder.run();
        sb.append(" }");
        sb.append(" }");
    }

    @Override
    public QueryResult buildInsert(AnnotationMetadata repositoryMetadata, PersistentEntity entity) {
        return null;
    }

    @Override
    public QueryResult buildQuery(AnnotationMetadata annotationMetadata, QueryModel query) {
        ArgumentUtils.requireNonNull("annotationMetadata", annotationMetadata);
        ArgumentUtils.requireNonNull("query", query);

        QueryState queryState = new QueryState(query, true, false);

        QueryModel.Junction criteria = query.getCriteria();

        if (!criteria.isEmpty()) {
            buildWhereClause(annotationMetadata, criteria, queryState);
        }

        return QueryResult.of(
                queryState.getFinalQuery(),
                queryState.getQueryParts(),
                queryState.getParameterBindings(),
                queryState.getAdditionalRequiredParameters(),
                query.getMax(),
                query.getOffset()
        );
    }

    private void buildWhereClause(AnnotationMetadata annotationMetadata, QueryModel.Junction criteria, QueryState queryState) {
        if (!criteria.isEmpty()) {

            CriteriaContext ctx = new CriteriaContext() {

                @Override
                public String getCurrentTableAlias() {
                    return queryState.getRootAlias();
                }

                @Override
                public QueryState getQueryState() {
                    return queryState;
                }

                @Override
                public PersistentEntity getPersistentEntity() {
                    return queryState.getEntity();
                }

                @Override
                public QueryPropertyPath getRequiredProperty(String name, Class<?> criterionClazz) {
                    return findProperty(queryState, name, criterionClazz);
                }

            };

            handleCriterion(ctx, queryState.query, criteria);
        }
    }

    @NonNull
    private QueryPropertyPath findProperty(QueryState queryState, String name, Class criterionType) {
        return findPropertyInternal(queryState, queryState.getEntity(), queryState.getRootAlias(), name, criterionType);
    }

    private QueryPropertyPath findPropertyInternal(QueryState queryState, PersistentEntity entity, String tableAlias, String name, Class criterionType) {
        PersistentPropertyPath propertyPath = entity.getPropertyPath(name);
        if (propertyPath != null) {
            if (propertyPath.getAssociations().isEmpty()) {
                return new QueryPropertyPath(propertyPath, tableAlias);
            }
            Association joinAssociation = null;
            StringJoiner joinPathJoiner = new StringJoiner(".");
            String lastJoinAlias = null;
            for (Association association : propertyPath.getAssociations()) {
                joinPathJoiner.add(association.getName());
                if (association instanceof Embedded) {
                    continue;
                }
                if (joinAssociation == null) {
                    joinAssociation = association;
                    continue;
                }
                if (association != joinAssociation.getAssociatedEntity().getIdentity()) {
                    if (!queryState.isAllowJoins()) {
                        throw new IllegalArgumentException("Joins cannot be used in a DELETE or UPDATE operation");
                    }
                    String joinStringPath = joinPathJoiner.toString();
                    if (!queryState.isJoined(joinStringPath)) {
                        throw new IllegalArgumentException("Property is not joined at path: " + joinStringPath);
                    }
//                    lastJoinAlias = joinInPath(queryState, joinStringPath);
                    // Continue to look for a joined property
                    joinAssociation = association;
                } else {
                    // We don't need to join to access the id of the relation
                    joinAssociation = null;
                }
            }
            PersistentProperty property = propertyPath.getProperty();
            if (joinAssociation != null) {
                if (property != joinAssociation.getAssociatedEntity().getIdentity()) {
                    String joinStringPath = joinPathJoiner.toString();
                    if (!queryState.isJoined(joinStringPath)) {
                        throw new IllegalArgumentException("Property is not joined at path: " + joinStringPath);
                    }
                    if (lastJoinAlias == null) {
//                        lastJoinAlias = joinInPath(queryState, joinPathJoiner.toString());
                    }
                    // 'joinPath.prop' should be represented as a path of 'prop' with a join alias
                    return new QueryPropertyPath(
                            new PersistentPropertyPath(Collections.emptyList(), property, property.getName()),
                            lastJoinAlias
                    );
                }
                // We don't need to join to access the id of the relation
            }
        } else if (TypeRole.ID.equals(name) && entity.getIdentity() != null) {
            // special case handling for ID
            return new QueryPropertyPath(
                    new PersistentPropertyPath(Collections.emptyList(), entity.getIdentity(), entity.getIdentity().getName()),
                    queryState.getRootAlias()
            );
        }
        if (propertyPath == null) {
            if (criterionType == null || criterionType == Sort.Order.class) {
                throw new IllegalArgumentException("Cannot order on non-existent property path: " + name);
            } else {
                throw new IllegalArgumentException("Cannot use [" + criterionType.getSimpleName() + "] criterion on non-existent property path: " + name);
            }
        }
        return new QueryPropertyPath(propertyPath, tableAlias);
    }

    private void handleJunction(CriteriaContext ctx, StringBuilder sb, QueryModel.Junction criteria, String operator) {
        if (criteria.getCriteria().size() == 1) {
            handleCriterion(ctx, sb, criteria.getCriteria().iterator().next());
        } else {
            sb.append("{ ");
            sb.append(operator).append(": ");
            Iterator<QueryModel.Criterion> iterator = criteria.getCriteria().iterator();
            sb.append("[ ");
            while (iterator.hasNext()) {
                handleCriterion(ctx, sb, iterator.next());
                if (iterator.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(" ]");
            sb.append(" }");
        }
    }

    private void handleCriterion(CriteriaContext ctx, StringBuilder sb, QueryModel.Criterion criterion) {
        CriterionHandler<QueryModel.Criterion> criterionHandler = queryHandlers.get(criterion.getClass());
        if (criterionHandler == null) {
            throw new IllegalArgumentException("Queries of type " + criterion.getClass().getSimpleName() + " are not supported by this implementation");
        }
        criterionHandler.handle(ctx, sb, criterion);
    }

    @Override
    public QueryResult buildUpdate(AnnotationMetadata annotationMetadata, QueryModel query, List<String> propertiesToUpdate) {
        return null;
    }

    @Override
    public QueryResult buildUpdate(AnnotationMetadata annotationMetadata, QueryModel query, Map<String, Object> propertiesToUpdate) {
        return null;
    }

    @Override
    public QueryResult buildDelete(AnnotationMetadata annotationMetadata, QueryModel query) {
        return null;
    }

    @Override
    public QueryResult buildOrderBy(PersistentEntity entity, Sort sort) {
        return null;
    }

    @Override
    public QueryResult buildPagination(Pageable pageable) {
        return null;
    }

    /**
     * Adds criterion handler.
     *
     * @param clazz   The handler class
     * @param handler The handler
     * @param <T>     The criterion type
     */
    protected <T extends QueryModel.Criterion> void addCriterionHandler(Class<T> clazz, CriterionHandler<T> handler) {
        queryHandlers.put(clazz, handler);
    }

    private BindingParameter.BindingContext newBindingContext(@Nullable PersistentPropertyPath ref,
                                                              @Nullable PersistentPropertyPath persistentPropertyPath) {
        return BindingParameter.BindingContext.create()
                .incomingMethodParameterProperty(ref)
                .outgoingQueryParameterProperty(persistentPropertyPath)
                .expandable();
    }

    private BindingParameter.BindingContext newBindingContext(@Nullable PersistentPropertyPath ref) {
        return BindingParameter.BindingContext.create()
                .incomingMethodParameterProperty(ref)
                .outgoingQueryParameterProperty(ref)
                .expandable();
    }

    protected Placeholder formatParameter(int index) {
        return new Placeholder("?", String.valueOf(index));
    }

    /**
     * A criterion handler.
     *
     * @param <T> The criterion type
     */
    protected interface CriterionHandler<T extends QueryModel.Criterion> {

        /**
         * Handles a criterion.
         *
         * @param context   The context
         * @param criterion The criterion
         */
        void handle(CriteriaContext context, StringBuilder sb, T criterion);
    }

    /**
     * A criterion context.
     */
    protected interface CriteriaContext extends PropertyParameterCreator {

        String getCurrentTableAlias();

        QueryState getQueryState();

        PersistentEntity getPersistentEntity();

        QueryPropertyPath getRequiredProperty(String name, Class<?> criterionClazz);

        default void pushParameter(@NotNull BindingParameter bindingParameter, @NotNull BindingParameter.BindingContext bindingContext) {
            getQueryState().pushParameter(bindingParameter, bindingContext);
        }

        default QueryPropertyPath getRequiredProperty(QueryModel.PropertyNameCriterion propertyCriterion) {
            return getRequiredProperty(propertyCriterion.getProperty(), propertyCriterion.getClass());
        }

    }

    /**
     * The state of the query.
     */
    @Internal
    protected final class QueryState implements PropertyParameterCreator {
        private final String rootAlias;
        private final Map<String, String> appliedJoinPaths = new HashMap<>();
        private final AtomicInteger position = new AtomicInteger(0);
        private final Map<String, String> additionalRequiredParameters = new LinkedHashMap<>();
        private final List<QueryParameterBinding> parameterBindings;
        private final StringBuilder query = new StringBuilder();
        private final List<String> queryParts = new ArrayList<>();
        private final boolean allowJoins;
        private final QueryModel queryObject;
        private final boolean escape;
        private final PersistentEntity entity;

        private QueryState(QueryModel query, boolean allowJoins, boolean useAlias) {
            this.allowJoins = allowJoins;
            this.queryObject = query;
            this.entity = query.getPersistentEntity();
            this.escape = shouldEscape();
            this.rootAlias = useAlias ? null : null;
            this.parameterBindings = new ArrayList<>(entity.getPersistentPropertyNames().size());
        }

        /**
         * @return The root alias
         */
        public @Nullable
        String getRootAlias() {
            return rootAlias;
        }

        /**
         * @return The entity
         */
        public PersistentEntity getEntity() {
            return entity;
        }

//        /**
//         * Add a required parameter.
//         *
//         * @param name The name
//         * @return name A placeholder in a query
//         */
//        public String addAdditionalRequiredParameter(@NonNull String name) {
//            AbstractSqlLikeQueryBuilder.Placeholder placeholder = newParameter();
//            additionalRequiredParameters.put(placeholder.key, name);
//            return placeholder.name;
//        }

        public String getFinalQuery() {
            if (query.length() > 0) {
                queryParts.add(query.toString());
                query.setLength(0);
            }
            if (queryParts.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder(queryParts.get(0));
            int i = 1;
            for (int k = 1; k < queryParts.size(); k++) {
                Placeholder placeholder = formatParameter(i++);
                sb.append(placeholder.name);
                sb.append(queryParts.get(k));
            }
            return sb.toString();
        }

        public List<String> getQueryParts() {
            return queryParts;
        }

        /**
         * @return The query string
         */
        public StringBuilder getQuery() {
            return query;
        }

        /**
         * @return Does the query allow joins
         */
        public boolean isAllowJoins() {
            return allowJoins;
        }

        /**
         * @return The query model object
         */
        public QueryModel getQueryModel() {
            return queryObject;
        }

        /**
         * Constructs a new parameter placeholder.
         *
         * @return The parameter
         */
        private Placeholder newParameter() {
            return formatParameter(position.incrementAndGet());
        }

//        /**
//         * Applies a join for the given association.
//         *
//         * @param jp The join path
//         * @return The alias
//         */
//        public String applyJoin(@NonNull JoinPath jp) {
//            String joinAlias = appliedJoinPaths.get(jp.getPath());
//            if (joinAlias != null) {
//                return joinAlias;
//            }
//            Optional<JoinPath> ojp = getQueryModel().getJoinPath(jp.getPath());
//            if (ojp.isPresent()) {
//                jp = ojp.get();
//            }
//            StringBuilder stringBuilder = getQuery();
//            Join.Type jt = jp.getJoinType();
//            String joinType = resolveJoinType(jt);
//
//            String[] associationAlias = buildJoin(
//                    getRootAlias(),
//                    jp,
//                    joinType,
//                    stringBuilder,
//                    appliedJoinPaths,
//                    this);
//            Association[] associationArray = jp.getAssociationPath();
//            StringJoiner associationPath = new StringJoiner(".");
//            String lastAlias = null;
//            for (int i = 0; i < associationAlias.length; i++) {
//                associationPath.add(associationArray[i].getName());
//                String computedAlias = associationAlias[i];
//                appliedJoinPaths.put(associationPath.toString(), computedAlias);
//                lastAlias = computedAlias;
//            }
//            return lastAlias;
//        }
//

        /**
         * Checks if the path is joined already.
         *
         * @param associationPath The association path.
         * @return true if joined
         */
        public boolean isJoined(String associationPath) {
            for (String joinPath : appliedJoinPaths.keySet()) {
                if (joinPath.startsWith(associationPath)) {
                    return true;
                }
            }
            return appliedJoinPaths.containsKey(associationPath);
        }

        /**
         * @return Should escape the query
         */
        public boolean shouldEscape() {
            return escape;
        }

        /**
         * The additional required parameters.
         *
         * @return The parameters
         */
        public @NotNull Map<String, String> getAdditionalRequiredParameters() {
            return this.additionalRequiredParameters;
        }

        /**
         * The parameter binding.
         *
         * @return The parameter binding
         */
        public List<QueryParameterBinding> getParameterBindings() {
            return parameterBindings;
        }

        @Override
        public void pushParameter(@NotNull BindingParameter bindingParameter, @NotNull BindingParameter.BindingContext bindingContext) {
            Placeholder placeholder = newParameter();
            bindingContext = bindingContext
                    .index(position.get() + 1)
                    .name(placeholder.getKey());
            parameterBindings.add(
                    bindingParameter.bind(bindingContext)
            );
            queryParts.add(query.toString());
            query.setLength(0);
        }
    }

    private interface PropertyParameterCreator {

        void pushParameter(@NotNull BindingParameter bindingParameter,
                           @NotNull BindingParameter.BindingContext bindingContext);

    }

    /**
     * Represents a placeholder in query.
     */
    public static final class Placeholder {
        private final String name;
        private final String key;

        /**
         * Default constructor.
         *
         * @param name The name of the place holder
         * @param key  The key to set the value of the place holder
         */
        public Placeholder(String name, String key) {
            this.name = name;
            this.key = key;
        }

        @Override
        public String toString() {
            return name;
        }

        /**
         * @return The place holder name
         */
        public String getName() {
            return name;
        }

        /**
         * This the precomputed key to set the place holder. In SQL this would be the index.
         *
         * @return The key used to set the placeholder.
         */
        public String getKey() {
            return key;
        }
    }

    /**
     * Represents a path to a property.
     */
    protected class QueryPropertyPath {
        private final PersistentPropertyPath propertyPath;
        private final String tableAlias;

        /**
         * Default constructor.
         *
         * @param propertyPath The propertyPath
         * @param tableAlias   The tableAlias
         */
        public QueryPropertyPath(@NotNull PersistentPropertyPath propertyPath, @Nullable String tableAlias) {
            this.propertyPath = propertyPath;
            this.tableAlias = tableAlias;
        }

        /**
         * @return The associations
         */
        @NonNull
        public List<Association> getAssociations() {
            return propertyPath.getAssociations();
        }

        /**
         * @return The property
         */
        @NonNull
        public PersistentProperty getProperty() {
            return propertyPath.getProperty();
        }

        /**
         * @return The path
         */
        @NonNull
        public String getPath() {
            return propertyPath.getPath();
        }

        /**
         * @return The path
         */
        @Nullable
        public String getTableAlias() {
            return tableAlias;
        }

//        /**
//         * @return already escaped column name
//         */
//        public String getColumnName() {
//            String columnName = getNamingStrategy().mappedName(propertyPath.getAssociations(), propertyPath.getProperty());
//            if (shouldEscape()) {
//                return quote(columnName);
//            }
//            return columnName;
//        }

        /**
         * @return the naming strategy
         */
        public NamingStrategy getNamingStrategy() {
            return propertyPath.getNamingStrategy();
        }

    }
}
