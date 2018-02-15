/*
 * Copyright 2016 Onshape, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.memcached.jcache;

import com.diffplug.common.base.Errors;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.OperationsException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.commons.lang3.StringUtils;

public class MemcachedCache<K, V> implements javax.cache.Cache<K, V> {
  private static final Logger LOG = Logger.getLogger(MemcachedCache.class.getName());

  private final String cacheName;
  private final CompleteConfiguration<K, V> configuration;
  private final CacheManager cacheManager;
  private final Statistics statistics = new Statistics();
  private final String servers;
  private final MemcachedCacheLoader cacheLoader;
  private final int expiry;
  private final AtomicBoolean closed = new AtomicBoolean();
  private Transcoder<V> transcoder;
  private MemcachedClient client;
  private ExecutorService executorService;

  public MemcachedCache(
      String cacheName, CompleteConfiguration<K, V> configuration, CacheManager cacheManager) {
    this.cacheName = cacheName;
    this.configuration = configuration;
    this.cacheManager = cacheManager;

    Properties properties = cacheManager.getProperties();

    if (configuration.isManagementEnabled()) {
      MemcachedCacheMXBean bean = new MemcachedCacheMXBean(this);

      try {
        ManagementFactory.getPlatformMBeanServer()
            .registerMBean(bean, new ObjectName(bean.getObjectName()));
      } catch (OperationsException | MBeanException e) {
        throw new CacheException(e);
      }
    }

    servers = properties.getProperty("servers", "127.0.0.1:11211");
    expiry = Integer.parseUnsignedInt(properties.getProperty("expiry", "3600"));

    if (configuration.isReadThrough()) {
      Factory<CacheLoader<K, V>> factory = configuration.getCacheLoaderFactory();

      cacheLoader = new MemcachedCacheLoader<>(factory.create());
    } else {
      cacheLoader = null;
    }

    if (configuration.isStatisticsEnabled()) {
      MemcachedCacheStatisticsMXBean bean = new MemcachedCacheStatisticsMXBean(statistics);

      try {
        ManagementFactory.getPlatformMBeanServer()
            .registerMBean(bean, new ObjectName(bean.getObjectName(this)));
      } catch (OperationsException | MBeanException e) {
        throw new CacheException(e);
      }
    }

    ConnectionFactory connectionFactory = new BinaryConnectionFactory();
    transcoder = (Transcoder<V>) connectionFactory.getDefaultTranscoder();
    try {
      String className = property(properties, "serializer", cacheName, "");
      if (!StringUtils.isBlank(className)) {
        try {
          transcoder =
              Transcoder.class.cast(
                  this.getClass().getClassLoader().loadClass(className).newInstance());
        } catch (ClassNotFoundException e) {
          LOG.warning(
              String.format(
                  "Unable to load transcoder class name {} for class {}. {}",
                  className,
                  cacheName,
                  e));
          throw e;
        }
      }
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }

    synchronized (closed) {
      closed.set(true);
      client = connect(connectionFactory, properties);
    }
  }

  private MemcachedClient connect(ConnectionFactory connectionFactory, Properties properties) {
    MemcachedClient client = null;
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdownNow();
    }
    final int poolSize = Integer.parseInt(property(properties, cacheName, "pool.size", "3"));
    final DaemonThreadFactory threadFactory =
        new DaemonThreadFactory("MemcacheD-JCache-" + cacheName + "-");
    executorService =
        poolSize > 0
            ? Executors.newFixedThreadPool(poolSize, threadFactory)
            : Executors.newCachedThreadPool(threadFactory);
    if (servers == null) {
      throw new IllegalArgumentException("servers is null");
    }
    List<InetSocketAddress> addresses = AddrUtil.getAddresses(servers);
    if (addresses == null || addresses.size() < 1) {
      LOG.warning(
          "Invalid or missing server addresses in properties, defaulting to 127.0.0.1:11211");
      addresses = AddrUtil.getAddresses("127.0.0.1:11211");
    }

    try {
      client = new MemcachedClient(connectionFactory, addresses);
      client.addObserver(
          new ConnectionObserver() {
            @Override
            public void connectionEstablished(SocketAddress socketAddress, int i) {
              closed.set(false);
            }

            @Override
            public void connectionLost(SocketAddress socketAddress) {
              closed.set(true);
            }
          });
      closed.set(false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return client;
  }

  private static String property(
      final Properties properties,
      final String cacheName,
      final String name,
      final String defaultValue) {
    return properties.getProperty(
        cacheName + "." + name, properties.getProperty(name, defaultValue));
  }

  @Override
  public V get(K key) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();
    final Timer timer = Timer.start(!statisticsEnabled);

    if (key == null) {
      throw new NullPointerException();
    }

    V value = client.<V>get(key.toString(), transcoder);
    if (statisticsEnabled) {
      if (value != null) {
        statistics.increaseHits(1);
      } else {
        statistics.increaseMisses(1);
      }
    }

    if (value == null && cacheLoader != null && configuration.isReadThrough()) {
      value = (V) cacheLoader.load(key);
      client.set(key.toString(), expiry, value, transcoder);
    }

    if (statisticsEnabled) {
      statistics.addGetTime(timer.elapsed());
    }
    return value;
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
    checkState();

    if (keys == null || keys.contains(null)) {
      throw new NullPointerException();
    }

    Map<String, V> kvp =
        client.<V>getBulk(
            keys.stream().<String>map(key -> key.toString()).collect(Collectors.toSet()),
            transcoder);

    if (configuration.isReadThrough() && cacheLoader != null) {
      Map<String, V> lkvp =
          cacheLoader.loadAll(
              keys.stream().filter(key -> !kvp.containsKey(key)).collect(Collectors.toSet()));
      lkvp.entrySet()
          .parallelStream()
          .forEach(entry -> client.set(entry.getKey(), expiry, entry.getValue(), transcoder));
      kvp.putAll(lkvp);
    }

    return (Map<K, V>) kvp;
  }

  @Override
  public boolean containsKey(K key) {
    checkState();

    if (key == null) {
      throw new NullPointerException();
    }

    V value = client.<V>get(key.toString(), transcoder);

    return value != null;
  }

  @Override
  public void loadAll(
      final Set<? extends K> keys,
      final boolean replaceExistingValues,
      final CompletionListener cl) {
    checkState();

    if (keys == null || cl == null) {
      throw new NullPointerException();
    }

    keys.parallelStream()
        .forEach(
            key -> {
              try {
                Object value = client.get(key.toString());
                if (value == null || replaceExistingValues) {
                  V loadedValue = (V) cacheLoader.load(key);
                  client.add(key.toString(), expiry, loadedValue, transcoder);
                }
              } catch (Exception e) {
                cl.onException(e);
              } finally {
                cl.onCompletion();
              }
            });
  }

  @Override
  public void put(K key, V value) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();
    final Timer timer = Timer.start(!statisticsEnabled);

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    OperationFuture<Boolean> of = client.set(key.toString(), expiry, value);
    try {
      of.get();
      if (statisticsEnabled) {
        statistics.increasePuts(1);
        statistics.addGetTime(timer.elapsed());
      }
    } catch (InterruptedException | ExecutionException e) {
      of.cancel();
    }
  }

  @Override
  public V getAndPut(K key, V value) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();

    if (key == null) {
      throw new NullPointerException();
    }

    V previousValue = client.<V>get(key.toString(), transcoder);
    if (previousValue == null && cacheLoader != null && configuration.isReadThrough()) {
      previousValue = (V) cacheLoader.load(key);
      // NOTE: skip setting the value here, we're about to change it
      // client.set(key.toString(), expiry, previousValue, transcoder);
    }

    final Timer timer = Timer.start(!statisticsEnabled);
    client.set(key.toString(), expiry, value, transcoder);

    if (statisticsEnabled) {
      timer.stop();
      statistics.increasePuts(1);
      statistics.addPutTime(timer.elapsed());
    }
    return previousValue;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    checkState();

    if (map == null || map.containsKey(null) || map.containsValue(null)) {
      throw new NullPointerException();
    }

    map.entrySet()
        .parallelStream()
        .map(
            entry ->
                asCompletableFuture(
                    client.set(entry.getKey().toString(), expiry, entry.getValue())))
        .forEach(cf -> cf.thenAccept(v -> {}));
  }

  @Override
  public boolean putIfAbsent(K key, V value) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();
    final Timer timer = Timer.start(!statisticsEnabled);

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    boolean applied;
    try {
      applied = client.add(key.toString(), expiry, value, transcoder).get();
    } catch (ExecutionException | InterruptedException e) {
      applied = false;
    }

    if (statisticsEnabled) {
      statistics.increasePuts(1);
      statistics.addPutTime(timer.elapsed());
    }
    return applied;
  }

  @Override
  public boolean remove(K key) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();
    final Timer timer = Timer.start(!statisticsEnabled);

    if (key == null) {
      throw new NullPointerException();
    }

    boolean applied = false;
    try {
      applied = this.<Boolean>asCompletableFuture(client.delete(key.toString())).get();
      if (statisticsEnabled) {
        statistics.increaseRemovals(1);
        statistics.addRemoveTime(timer.elapsed());
      }
    } catch (InterruptedException | ExecutionException e) {
      applied = false;
    }

    return applied;
  }

  @Override
  public boolean remove(K key, V oldValue) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();
    final Timer timer = Timer.start(!statisticsEnabled);

    if (key == null || oldValue == null) {
      throw new NullPointerException();
    }

    boolean applied = false;
    CASValue<V> casv = client.gets(key.toString(), transcoder);
    if (casv != null && oldValue.equals(casv.getValue())) {
      try {
        applied = client.delete(key.toString(), casv.getCas()).get();
      } catch (ExecutionException | InterruptedException e) {
        applied = false;
      }
    }

    if (statisticsEnabled) {
      statistics.increaseRemovals(1);
      statistics.addRemoveTime(timer.elapsed());
    }
    return applied;
  }

  @Override
  public V getAndRemove(K key) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();

    if (key == null) {
      throw new NullPointerException();
    }

    CASValue<V> casv = client.gets(key.toString(), transcoder);

    final Timer timer = Timer.start(!statisticsEnabled);
    V value = null;
    if (casv != null) {

      if (asCompletableFuture(client.delete(key.toString(), casv.getCas()))
          .exceptionally(
              ex -> {
                LOG.info(
                    String.format(
                        "Unable to remove key {} from cache {}.", key.toString(), cacheName));
                return false;
              })
          .join()) {
        if (statisticsEnabled) {
          statistics.increaseRemovals(1);
          statistics.addRemoveTime(timer.elapsed());
        }
        value = casv.getValue();
      }
    }

    return value;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();

    if (key == null || oldValue == null || newValue == null) {
      throw new NullPointerException();
    }

    boolean applied = false;
    CASValue<V> casv = client.gets(key.toString(), transcoder);

    final Timer timer = Timer.start(!statisticsEnabled);
    if (casv != null && oldValue.equals(casv.getValue())) {
      timer.reset();
      CASResponse casr = client.cas(key.toString(), casv.getCas(), expiry, newValue, transcoder);
      applied = (casr == CASResponse.OK);
      if (!applied) {
        LOG.info(
            String.format("Unable to replace key {} from cache {}.", key.toString(), cacheName));
      }
      if (statisticsEnabled) {
        statistics.increaseRemovals(1);
        statistics.addRemoveTime(timer.elapsed());
      }
    }

    return applied;
  }

  @Override
  public boolean replace(K key, V value) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();
    final Timer timer = Timer.start(!statisticsEnabled);

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    boolean applied = false;
    try {
      applied = client.replace(key.toString(), expiry, value, transcoder).get();
    } catch (ExecutionException | InterruptedException e) {
      applied = false;
    }

    if (statisticsEnabled && applied) {
      statistics.increaseRemovals(1);
      statistics.addRemoveTime(timer.elapsed());
    }
    return applied;
  }

  @Override
  public V getAndReplace(K key, V value) {
    checkState();
    final boolean statisticsEnabled = configuration.isStatisticsEnabled();

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    boolean applied = false;
    CASValue<V> casv = client.gets(key.toString(), transcoder);

    final Timer timer = Timer.start(!statisticsEnabled);
    if (casv != null && casv.getValue() != null) {
      applied =
          client.cas(key.toString(), casv.getCas(), expiry, value, transcoder) == CASResponse.OK;
    }
    if (applied) {
      if (statisticsEnabled) {
        statistics.increaseRemovals(1);
        statistics.addRemoveTime(timer.elapsed());
      }
      return casv.getValue();
    }
    return null;
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    checkState();

    if (keys == null || keys.contains(null)) {
      throw new NullPointerException();
    }

    keys.parallelStream().forEach(key -> client.delete(key.toString()));
  }

  @Override
  public void removeAll() {
    checkState();

    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    checkState();

    client.flush();
  }

  @Override
  public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
    return (C) configuration;
  }

  @Override
  public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
      throws EntryProcessorException {
    checkState();

    if (key == null || entryProcessor == null) {
      throw new NullPointerException();
    }

    V value = get(key);

    MemcachedMutableEntry<K, V> entry = new MemcachedMutableEntry<>(key, value);

    T t = entryProcessor.process(entry, arguments);

    if (entry.isUpdated()) {
      replace(key, entry.getValue());
    }

    if (entry.isRemoved()) {
      remove(key);
    }

    return t;
  }

  @Override
  public <T> Map<K, EntryProcessorResult<T>> invokeAll(
      Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
    Map<K, EntryProcessorResult<T>> results = new HashMap<>();

    for (K key : keys) {
      results.put(key, new MemcachedEntryProcessorResult<>(invoke(key, entryProcessor, arguments)));
    }

    return results;
  }

  @Override
  public String getName() {
    return cacheName;
  }

  @Override
  public CacheManager getCacheManager() {
    return cacheManager;
  }

  @Override
  public void close() {
    synchronized (closed) {
      if (closed.compareAndSet(false, true)) {

        client.shutdown();
        ((MemcachedCacheManager) cacheManager).close(this);

        if (configuration.isManagementEnabled()) {
          String name = MemcachedCacheMXBean.getObjectName(this);

          try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(name));
          } catch (OperationsException | MBeanException e) {
            throw new CacheException(e);
          }
        }

        if (configuration.isStatisticsEnabled()) {
          String name = MemcachedCacheStatisticsMXBean.getObjectName(this);

          try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(name));
          } catch (OperationsException | MBeanException e) {
            throw new CacheException(e);
          }
        }
      }
    }
  }

  @Override
  public boolean isClosed() {
    synchronized (closed) {
      return closed.get();
    }
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (!clazz.isAssignableFrom(getClass())) {
      throw new IllegalArgumentException();
    }

    return clazz.cast(this);
  }

  @Override
  public void registerCacheEntryListener(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void deregisterCacheEntryListener(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    throw new UnsupportedOperationException();
  }

  private void checkState() {
    if (isClosed()) {
      throw new IllegalStateException("This cache is closed!");
    }
  }

  private static <T> CompletableFuture<T> asCompletableFuture(Future<T> future) {
    return asCompletableFuture(future, 500, TimeUnit.MILLISECONDS);
  }

  private static <T> CompletableFuture<T> asCompletableFuture(
      Future<T> future, long timeout, TimeUnit units) {
    return CompletableFuture.supplyAsync(
        Errors.rethrow()
            .wrap(
                () -> {
                  try {
                    return future.get(timeout, units);
                  } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  private static final class DaemonThreadFactory implements ThreadFactory {
    private String prefix;
    private boolean threadIsDaemon = true;
    private int threadPriority = Thread.NORM_PRIORITY;

    /**
     * Constructor
     *
     * @param prefix thread name prefix
     */
    public DaemonThreadFactory(String prefix) {
      this(prefix, Thread.NORM_PRIORITY);
    }

    /**
     * Constructor
     *
     * @param prefix thread name prefix
     * @param threadPriority set thread priority
     */
    public DaemonThreadFactory(String prefix, int threadPriority) {
      this.prefix = prefix;
      this.threadPriority = threadPriority;
    }

    /**
     * Sets the thread to daemon.
     *
     * <p>
     *
     * @param runner
     * @return a daemon thread
     */
    public Thread newThread(Runnable runner) {
      Thread t = new Thread(runner);
      String oldName = t.getName();
      t.setName(prefix + oldName);
      t.setDaemon(threadIsDaemon);
      t.setPriority(threadPriority);
      return t;
    }
  }

  private static final class Timer {
    long started;
    long stopped = -1;

    public static Timer start(final boolean ignore) {
      if (!ignore) {
        return new Timer(System.nanoTime() / 1000);
      }
      return new Timer(-1);
    }

    private Timer(final long started) {
      this.started = started;
    }

    public void reset() {
      if (started != -1) {
        started = System.nanoTime() / 1000;
        stopped = -1;
      }
    }

    public void stop() {
      if (stopped == -1) {
        stopped = System.nanoTime() / 1000;
      }
    }

    public long started() {
      return started > -1 ? started : 0;
    }

    public long stopped() {
      return stopped == -1 ? 0 : stopped;
    }

    public long elapsed() {
      if (started > 0) {
        long now = System.nanoTime() / 1000;
        return now - started;
      }
      return 0;
    }

    public long get() {
      if (started > 0) {
        stop();
        return stopped - started;
      }
      return 0;
    }
  }
}
