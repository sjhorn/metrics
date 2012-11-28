package com.yammer.metrics.hazelcast.tests;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.hazelcast.InstrumentedHazelcast;

public class InstrumentedHazelcastTest {
    private HazelcastInstance hzInstance;
    private MetricsRegistry registry;
    
    @Before
    public void setUp() throws Exception {
        registry = new MetricsRegistry();
        hzInstance = Hazelcast.newHazelcastInstance(null);
    }
    
    @After
    public void tearDown() throws Exception {
        hzInstance.getLifecycleService().shutdown();
    }

    @Test
    public void mapStats() throws Exception {
        
        IMap<String, String> map1 = hzInstance.getMap("foo");
        
        InstrumentedHazelcast.instrument(registry, hzInstance);
        
        map1.put("key1", "value1");
        map1.put("key2", "value2");
        map1.put("key3", "value3");

        map1.get("key1");
        map1.get("key1");
        map1.get("key1");
        map1.get("key2");
        map1.get("key3");
        map1.get("keyX");
        map1.get("keyY");
        map1.get("keyZ");
        
        Map<String, String> stats = getStats();
        
        assertEquals("3", stats.get("foo.number-of-puts"));
        assertEquals("8", stats.get("foo.number-of-gets"));
        assertEquals("5", stats.get("foo.hits"));
        
        map1.get("keyA");
        map1.get("key3");
        
        stats = getStats();
        
        assertEquals("6", stats.get("foo.hits"));
    }
    
    @Test
    public void queueStats() throws Exception {
        IQueue<String> queue = hzInstance.getQueue("bar");
        
        InstrumentedHazelcast.instrument(registry, hzInstance);
        
        queue.add("hello");
        String item = queue.take();
        assertEquals("hello", item);
        item = queue.poll();
        assertEquals(null, item);
        
        Map<String, String> stats = getStats();
        
        assertEquals("2", stats.get("bar.number-of-polls"));
        assertEquals("1", stats.get("bar.number-of-offers"));
        assertEquals("1", stats.get("bar.number-of-empty-polls"));
    }
    
    @Test
    public void lockStats() throws Exception {
        // TODO
    }
    
    @Test
    public void semaphoreStats() throws Exception {
        // TODO
    }
    
    @Test
    public void countDownLatchStats() throws Exception {
        // TODO
    }
    
    @Test
    public void atomicNumberStats() throws Exception {
        // TODO
    }
    
    @Test
    public void topicStats() throws Exception {
        // TODO
    }
    
    private Map<String, String> getStats() throws Exception {
        Thread.sleep(1); // Seems to be an async lag on hz metrics
        
        Map<String, String> stats = new HashMap<String, String>(); 
        for(Entry<MetricName, Metric>  entry : registry.getAllMetrics().entrySet()) {
            Gauge<?> guage = (Gauge<?>) entry.getValue();
            MetricName name = entry.getKey();
            stats.put(name.getScope()+"."+name.getName(), guage.getValue().toString());
        }
        return stats;
    }
    
    private void printStats() throws Exception {
        for(Entry<String,String> entry: getStats().entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }
}
