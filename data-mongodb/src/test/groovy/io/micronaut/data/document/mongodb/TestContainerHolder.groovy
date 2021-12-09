package io.micronaut.data.document.mongodb

import org.testcontainers.containers.MongoDBContainer

import java.util.concurrent.atomic.AtomicInteger

class TestContainerHolder {

    private static MongoDBContainer container
    private static AtomicInteger usedCounter = new AtomicInteger()

    static MongoDBContainer getContainerOrCreate() {
        if (container == null) {
            container = new MongoDBContainer()
            usedCounter = new AtomicInteger()
        } else {
            usedCounter.intValue()
        }
        return container
    }

    static void cleanup( int cleanupOnCount) {
        if (usedCounter.incrementAndGet() == cleanupOnCount) {
            container.stop()
        }
    }

}
