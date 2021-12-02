package io.micronaut.data.runtime.operations.internal;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.data.annotation.Relation;
import io.micronaut.data.event.EntityEventListener;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.exceptions.OptimisticLockException;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;

/**
 * The entity operations container.
 *
 * @param <T> The entity type
 */
abstract class EntityOperations<T, Exc extends Exception> extends BaseOperations<T, Exc> {

    EntityOperations(EntityEventListener<Object> entityEventListener, RuntimePersistentEntity<T> persistentEntity, ConversionService<?> conversionService) {
        super(entityEventListener, persistentEntity, conversionService);
    }

    /**
     * Persist one operation.
     */
    public void persist() {
        try {
//            if (QUERY_LOG.isDebugEnabled()) {
//                QUERY_LOG.debug("Executing SQL Insert: {}", op.debug());
//            }
            boolean vetoed = triggerPrePersist();
            if (vetoed) {
                return;
            }
            cascadePre(Relation.Cascade.PERSIST);
            execute();
            triggerPostPersist();
            cascadePost(Relation.Cascade.PERSIST);
        } catch (Exception e) {
            throw new DataAccessException("SQL Error executing INSERT: " + e.getMessage(), e);
        }
    }

    /**
     * Delete one operation.
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
//                QUERY_LOG.debug("Executing SQL DELETE: {}", op.debug());
//            }
//            executeUpdate(this, connection, (entries, deleted) -> {
//                if (QUERY_LOG.isTraceEnabled()) {
//                    QUERY_LOG.trace("Delete operation deleted {} records", deleted);
//                }
//                if (op.getDbOperation().isOptimisticLock()) {
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
     * Update one operation.
     */
    public <T> void update() {
        collectAutoPopulatedPreviousValues();
        boolean vetoed = triggerPreUpdate();
        if (vetoed) {
            return;
        }
        cascadePre(Relation.Cascade.UPDATE);
        try {
//            if (QUERY_LOG.isDebugEnabled()) {
//                QUERY_LOG.debug("Executing SQL UPDATE: {}", op.debug());
//            }
//            executeUpdate(this, connection, (entries, rowsUpdated) -> {
//                if (QUERY_LOG.isTraceEnabled()) {
//                    QUERY_LOG.trace("Update operation updated {} records", rowsUpdated);
//                }
//                if (op.getDbOperation().isOptimisticLock()) {
//                    checkOptimisticLocking(entries, rowsUpdated);
//                }
//            });
            execute();
            triggerPostUpdate();
            cascadePost(Relation.Cascade.UPDATE);
        } catch (OptimisticLockException ex) {
            throw ex;
        } catch (Exception e) {
            throw new DataAccessException("Error executing SQL UPDATE: " + e.getMessage(), e);
        }
    }

}
