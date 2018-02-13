package org.memcached.jcache;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.BeforeClass;

public class AbstractMemcachedTest {

  @BeforeClass
  public static void setup() throws IOException {
    int port = 11211;

    final MemCacheDaemon<LocalCacheElement> daemon = new MemCacheDaemon<LocalCacheElement>();

    CacheStorage<Key, LocalCacheElement> storage =
        ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 10000, 10000);
    daemon.setCache(new CacheImpl(storage));
    daemon.setAddr(new InetSocketAddress("localhost", port));
    daemon.setIdleTime(10000);
    daemon.setVerbose(true);
    daemon.start();

    System.setProperty("memcachedservers", "localhost:" + port);
  }
}
