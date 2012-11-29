package com.yammer.metrics.hazelcast;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Instance.InstanceType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * An instrumented {@link Hazelcast} instance.
 */
public class InstrumentedHazelcast {
    public static HazelcastInstance instrument(HazelcastInstance hzInstance) {
        return instrument(Metrics.defaultRegistry(), hzInstance);
    }
    
    public static HazelcastInstance instrument(MetricsRegistry registry,
            final HazelcastInstance hzInstance) {
        for (Instance instance : hzInstance.getInstances()) {
            InstanceType type = instance.getInstanceType();
            if (type.isMap() || type.isMultiMap()) {
                instrumentMap(registry, (IMap<?, ?>) instance);
            } else if (type.isQueue()) {
                instrumentQueue(registry, (IQueue<?>) instance);
            } else if (type.isLock()) {
                instrumentLock(registry, (ILock) instance);
            } else if (type.isAtomicNumber()) {
                instrumentAtomicNumber(registry, (AtomicNumber) instance);
            } else if (type.isCountDownLatch()) {
                instrumentLatch(registry, (ICountDownLatch) instance);
            } else if (type.isSemaphore()) {
                instrumentSemaphore(registry, (ISemaphore) instance);
            } else if (type.isTopic()) {
                instrumentTopic(registry, (ITopic) instance);
            }
        }
        // Todo: executor not sure where this one come from

        return hzInstance;
    }
    
    private static void instrumentSemaphore(MetricsRegistry registry,
            final ISemaphore semaphore) {
        String name = semaphore.getName();
        registry.newGauge(Hazelcast.class, "number-of-acquire-ops", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfAcquireOps();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-attached-permits", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfAttachedPermits();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-detached-permits", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfDetachedPermits();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-non-acquire-ops", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfNonAcquireOps();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-permits-acquired", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfPermitsAcquired();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-permits-released", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfPermitsReleased();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-rejected-acquires", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return semaphore.getLocalSemaphoreStats().getOperationStats()
                                .getNumberOfRejectedAcquires();
                    }
                });
    }
    
    private static void instrumentTopic(MetricsRegistry registry, final ITopic topic) {
        String name = topic.getName();
        registry.newGauge(Hazelcast.class, "number-of-publishes", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return topic.getLocalTopicStats().getOperationStats().getNumberOfPublishes();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-received-messages", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return topic.getLocalTopicStats().getOperationStats()
                                .getNumberOfReceivedMessages();
                    }
                });
    }

    private static void instrumentLatch(MetricsRegistry registry,
            final ICountDownLatch latch) {
        String name = latch.getName();
        registry.newGauge(Hazelcast.class, "number-of-awaits", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getNumberOfAwaits();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-awaits-released", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats()
                                .getNumberOfAwaitsReleased();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-count-downs", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getNumberOfCountDowns();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-gates-opened", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getNumberOfGatesOpened();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-others", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getNumberOfOthers();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-await-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getTotalAwaitLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-count-down-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getTotalCountDownLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-other-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latch.getLocalCountDownLatchStats()
                                .getOperationStats().getTotalOtherLatency();
                    }
                });
    }

    private static void instrumentAtomicNumber(MetricsRegistry registry,
            final AtomicNumber number) {
        String name = number.getName();
        registry.newGauge(Hazelcast.class, "number-of-modify-ops", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return number.getLocalAtomicNumberStats()
                                .getOperationStats().getNumberOfModifyOps();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-non-modify-ops", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return number.getLocalAtomicNumberStats()
                                .getOperationStats().getNumberOfNonModifyOps();
                    }
                });
    }

    private static void instrumentLock(MetricsRegistry registry, final ILock lock) {
        String name = "lock_"+Integer.toHexString(System.identityHashCode(lock));
        registry.newGauge(Hazelcast.class, "number-of-failed-locks", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return lock.getLocalLockStats().getOperationStats()
                                .getNumberOfFailedLocks();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-locks", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return lock.getLocalLockStats().getOperationStats()
                                .getNumberOfLocks();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-unlocks", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return lock.getLocalLockStats().getOperationStats()
                                .getNumberOfUnlocks();
                    }
                });
    }

    private static void instrumentMap(MetricsRegistry registry, final IMap<?,?> map) {
        String name = map.getName();
        registry.newGauge(Hazelcast.class, "backup-entry-count", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getBackupEntryCount();
                    }
                });
        registry.newGauge(Hazelcast.class, "backup-entry-memory-cost", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getBackupEntryMemoryCost();
                    }
                });
        registry.newGauge(Hazelcast.class, "creation-time", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getCreationTime();
                    }
                });
        registry.newGauge(Hazelcast.class, "dirty-entry-count", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getDirtyEntryCount();
                    }
                });
        registry.newGauge(Hazelcast.class, "hits", name, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return map.getLocalMapStats().getHits();
            }
        });
        registry.newGauge(Hazelcast.class, "last-access-time", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getLastAccessTime();
                    }
                });
        registry.newGauge(Hazelcast.class, "last-eviction-time", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getLastEvictionTime();
                    }
                });
        registry.newGauge(Hazelcast.class, "last-update-time", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getLastUpdateTime();
                    }
                });
        registry.newGauge(Hazelcast.class, "locked-entry-count", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getLastAccessTime();
                    }
                });
        registry.newGauge(Hazelcast.class, "lock-wait-count", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getLockWaitCount();
                    }
                });
        registry.newGauge(Hazelcast.class, "marked-as-removed-entry-count",
                name, new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getMarkedAsRemovedEntryCount();
                    }
                });
        registry.newGauge(Hazelcast.class, "marked-as-removed-memory-cost",
                name, new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getMarkedAsRemovedMemoryCost();
                    }
                });
        registry.newGauge(Hazelcast.class, "owned-entry-count", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOwnedEntryCount();
                    }
                });
        registry.newGauge(Hazelcast.class, "owned-entry-memory-cost", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOwnedEntryMemoryCost();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-events", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getNumberOfEvents();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-events", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getNumberOfEvents();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-gets", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getNumberOfGets();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-other-operations", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats()
                                .getNumberOfOtherOperations();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-puts", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getNumberOfPuts();
                        //return map.getLocalMapStats().getOperationStats().getNumberOfPuts();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-removes", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getNumberOfRemoves();
                    }
                });
        registry.newGauge(Hazelcast.class, "period-end", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getPeriodEnd();
                    }
                });
        registry.newGauge(Hazelcast.class, "period-start", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getPeriodStart();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-get-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getTotalGetLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-put-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats().getTotalPutLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-remove-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return map.getLocalMapStats().getOperationStats()
                                .getTotalRemoveLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total", name, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return map.getLocalMapStats().getOperationStats().total();
            }
        });
    }

    private static void instrumentQueue(MetricsRegistry registry,
            final IQueue queue) {
        String name = queue.getName();
        registry.newGauge(Hazelcast.class, "ave-age", name, new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getAveAge();
                    }
                });
        registry.newGauge(Hazelcast.class, "backup-item-count", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getAveAge();
                    }
                });
        registry.newGauge(Hazelcast.class, "max-age", name, new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getMaxAge();
                    }
                });
        registry.newGauge(Hazelcast.class, "min-age", name, new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getMinAge();
                    }
                });
        registry.newGauge(Hazelcast.class, "owned-item-count", name,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return queue.getLocalQueueStats().getOwnedItemCount();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-empty-polls", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getOperationStats()
                                .getNumberOfEmptyPolls();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-offers", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getOperationStats()
                                .getNumberOfOffers();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-polls", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getOperationStats()
                                .getNumberOfPolls();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-rejected-offers", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return queue.getLocalQueueStats().getOperationStats()
                                .getNumberOfRejectedOffers();
                    }
                });
        
    }
}