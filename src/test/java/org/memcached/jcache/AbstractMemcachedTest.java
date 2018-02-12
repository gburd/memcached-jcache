package org.memcached.jcache;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.cache.Cache;
import javax.cache.CacheManager;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.jcache.SpyCachingProvider;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;


public class AbstractMemcachedTest {
    @BeforeClass
    public static void setup() throws IOException {
        int port = 11212;
        // create daemon and start it
        final MemCacheDaemon<LocalCacheElement> daemon = new MemCacheDaemon<LocalCacheElement>();

        CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap
                .create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 10000,
                        10000);
        daemon.setCache(new CacheImpl(storage));
        // daemon.setBinary(true);
        daemon.setAddr(new InetSocketAddress("localhost", port));
        daemon.setIdleTime(10000);
        daemon.setVerbose(true);
        daemon.start();

        String servers = "localhost:" + port;
        System.setProperty("spymemcachedservers", servers);

        MemcachedCachingProvider provider = new MemcachedCachingProvider();
        assertNotNull(provider);

        // List<InetSocketAddress> addr = new ArrayList<InetSocketAddress>();
        // addr.add(new InetSocketAddress("localhost", port));
        // MemcachedClient client = new MemcachedClient(addr);
        // AddrUtil.getAddresses(servers));

        ClassLoader loader = MemcachedCache.class.getClassLoader();

        cacheManager = provider.getCacheManagerFactory().getCacheManager(
                loader, "default");
        assertNotNull(cacheManager);
        cache = cacheManager.<String, String> createCacheBuilder("cacheName")
                .build();
        assertNotNull(cache);
    }

}
