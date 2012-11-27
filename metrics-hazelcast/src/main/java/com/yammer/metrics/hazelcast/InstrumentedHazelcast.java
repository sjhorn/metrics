package com.yammer.metrics.hazelcast;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.monitor.LocalAtomicNumberStats;
import com.hazelcast.monitor.LocalCountDownLatchStats;
import com.hazelcast.monitor.LocalLockStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalQueueStats;
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
    
    public static HazelcastInstance instrument(MetricsRegistry registry, final HazelcastInstance hzInstance) {
        for( Instance instance : hzInstance.getInstances()) {
            InstanceType type = instance.getInstanceType();
            if(type.isMap() || type.isMultiMap()) {
                IMap<?,?> map = (IMap<?,?>) instance;
                instrumentMap(registry, map.getName(), map.getLocalMapStats() );
            } else if(type.isQueue()) {
                IQueue<?> queue = (IQueue<?>) instance;
                instrumentQueue(registry, queue.getName(), queue.getLocalQueueStats() );
            } else if(type.isLock()) {
                ILock lock = (ILock) instance;
                instrumentLock(registry, lock.getId().toString(), lock.getLocalLockStats() );
            } else if(type.isAtomicNumber()) {
                AtomicNumber number = (AtomicNumber) instance;
                instrumentAtomicNumber(registry, number.getName(), number.getLocalAtomicNumberStats() );
            } else if(type.isCountDownLatch()) {
                ICountDownLatch latch = (ICountDownLatch) instance;
                instrumentLatch(registry, latch.getName(), latch.getLocalCountDownLatchStats() );
            }
        }
        return hzInstance;
    }
    
    private static void instrumentLatch(MetricsRegistry registry, String name, final LocalCountDownLatchStats stats) {
        registry.newGauge(Hazelcast.class, "number-of-awaits", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getNumberOfAwaits();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-awaits-released", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getNumberOfAwaitsReleased();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-count-downs", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getNumberOfCountDowns();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-gates-opened", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getNumberOfGatesOpened();
                    }
                });
        registry.newGauge(Hazelcast.class, "number-of-others", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getNumberOfOthers();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-await-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getTotalAwaitLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-count-down-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getTotalCountDownLatency();
                    }
                });
        registry.newGauge(Hazelcast.class, "total-other-latency", name,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return stats.getOperationStats().getTotalOtherLatency();
                    }
                });
    }
    
    private static void instrumentAtomicNumber(MetricsRegistry registry, String name, final LocalAtomicNumberStats stats) {
        registry.newGauge(Hazelcast.class, "number-of-modify-ops", name,
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfModifyOps();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-non-modify-ops", name,
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfNonModifyOps();
                }
            }
        );
    }
    
    
    private static void instrumentLock(MetricsRegistry registry, String name, final LocalLockStats stats) {
        registry.newGauge(Hazelcast.class, "number-of-failed-locks", name,
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfFailedLocks();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-locks", name,
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfLocks();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-unlocks", name,
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfUnlocks();
                }
            }
        );
    }
    
    private static void instrumentMap(MetricsRegistry registry, String name, final LocalMapStats stats) {
        registry.newGauge(Hazelcast.class, "backup-entry-count", name,
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getBackupEntryCount();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "backup-entry-memory-cost", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getBackupEntryMemoryCost();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "creation-time", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getCreationTime();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "dirty-entry-count", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getDirtyEntryCount();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "hits", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getHits();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "last-access-time", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getLastAccessTime();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "last-eviction-time", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getLastEvictionTime();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "last-update-time", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getLastUpdateTime();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "locked-entry-count", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getLastAccessTime();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "lock-wait-count", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getLockWaitCount();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "marked-as-removed-entry-count", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getMarkedAsRemovedEntryCount();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "marked-as-removed-memory-cost", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getMarkedAsRemovedMemoryCost();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "owned-entry-count", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOwnedEntryCount();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "owned-entry-memory-cost", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOwnedEntryMemoryCost();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-events", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfEvents();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-events", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfEvents();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-gets", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfGets();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-other-operations", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfOtherOperations();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-puts", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfPuts();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "number-of-removes", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getNumberOfRemoves();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "period-end", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getPeriodEnd();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "period-start", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getPeriodStart();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "total-get-latency", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getTotalGetLatency();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "total-put-latency", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getTotalPutLatency();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "total-remove-latency", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().getTotalRemoveLatency();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "total", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getOperationStats().total();
                }
            }
        );
    }

    private static void instrumentQueue(MetricsRegistry registry,  String name, final LocalQueueStats stats) {
        registry.newGauge(Hazelcast.class, "ave-age", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getAveAge();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "backup-item-count", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getAveAge();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "max-age", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getMaxAge();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "min-age", name,
                new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return stats.getMinAge();
                }
            }
        );
        registry.newGauge(Hazelcast.class, "owned-item-count", name,
                new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return stats.getOwnedItemCount();
                }
            }
        );
    }
}