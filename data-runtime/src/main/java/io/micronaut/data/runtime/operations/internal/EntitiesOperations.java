package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.exceptions.OptimisticLockException;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;

/**
 * The entities operations container.
 *
 * @param <T> The entity type
 */
abstract class EntitiesOperations<T, Ext extends Exception> extends BaseOperations<T, Ext> {

    EntitiesOperations(EntityEventListener<Object> entityEventListener, RuntimePersistentEntity<T> persistentEntity, ConversionService<?> conversionService) {
        super(entityEventListener, persistentEntity, conversionService);
    }

    /**
     * Persist batch operation.
     */
    public void persist() {
        try {
            boolean allVetoed = triggerPrePersist();
            if (allVetoed) {
                return;
            }
            cascadePre(Relation.Cascade.PERSIST);
//            if (QUERY_LOG.isDebugEnabled()) {
//                QUERY_LOG.debug("Executing Batch SQL Insert: {}", op.debug());
//            }
            execute();
            triggerPostPersist();
            cascadePost(Relation.Cascade.PERSIST);
        } catch (Exception e) {
            throw new DataAccessException("SQL error executing INSERT: " + e.getMessage(), e);
        }
    }

    /**
     * Delete batch operation.
     */
    public void delete() {
        collectAutoPopulatedPreviousValues();
        boolean vetoed = triggerPreRemove();
        if (vetoed) {
            // operation vetoed
            return;
        }
        try {
//            if (QUERY_LOG.isDebugEnabled()) {
//                QUERY_LOG.debug("Executing Batch SQL DELETE: {}", op.debug());
//            }
//            executeUpdate(this, connection, (entries, deleted) -> {
//                if (QUERY_LOG.isTraceEnabled()) {
//                    QUERY_LOG.trace("Delete operation deleted {} records", deleted);
//                }
//                if (dbOperation.isOptimisticLock()) {
//                    checkOptimisticLocking(entries, deleted);
//                }
//            });
            execute();
            triggerPostRemove();
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL DELETE: " + e.getMessage(), e);
        }
    }

    /**
     * Update batch operation.
     */
    public <T> void update() {
        collectAutoPopulatedPreviousValues();
        triggerPreUpdate();
        try {
//            if (QUERY_LOG.isDebugEnabled()) {
//                QUERY_LOG.debug("Executing Batch SQL Update: {}", op.debug());
//            }
            cascadePre(Relation.Cascade.UPDATE);
//            executeUpdate(this, connection, (expected, updated) -> {
//                if (QUERY_LOG.isTraceEnabled()) {
//                    QUERY_LOG.trace("Update batch operation updated {} records", updated);
//                }
//                if (op.getDbOperation().isOptimisticLock()) {
//                    checkOptimisticLocking(expected, updated);
//                }
//            });
            execute();
            cascadePost(Relation.Cascade.UPDATE);
            triggerPostUpdate();
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL UPDATE: " + e.getMessage(), e);
        }
    }
}
