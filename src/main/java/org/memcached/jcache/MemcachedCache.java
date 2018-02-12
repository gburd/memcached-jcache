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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.OperationsException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;

public class MemcachedCache<K extends String, V> implements javax.cache.Cache<K, V> {
  private final String cacheName;
  private final CompleteConfiguration<K, V> configuration;
  private final CacheManager cacheManager;
  private final Statistics statistics = new Statistics();
  private final String servers;
  private final MemcachedCacheLoader cacheLoader;
  private final Transcoder<V> transcoder;
  private final int expiry;
  private MemcachedClient client;
  private ExecutorService executorService;

  //private final Set<CacheEntryListenerConfiguration<K, V>> cacheEntryListenerConfigurations;

  private final AtomicBoolean closed = new AtomicBoolean();

  public MemcachedCache(String cacheName, CompleteConfiguration<K, V> configuration, CacheManager cacheManager) {
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
        ManagementFactory.getPlatformMBeanServer().registerMBean(bean, new ObjectName(bean.getObjectName(this)));
      } catch (OperationsException | MBeanException e) {
        throw new CacheException(e);
      }
    }
    closed.set(true);
    client = connect(properties);
  }

  private MemcachedClient connect(Properties properties) {
      MemcachedClient client = null;
      if (executorService != null && !executorService.isShutdown()) {
          executorService.shutdownNow();
      }
      final int poolSize = Integer.parseInt(property(properties, cacheName, "pool.size", "3"));
      final DaemonThreadFactory threadFactory = new DaemonThreadFactory("MemcacheD-JCache-" + cacheName + "-");
      executorService = poolSize > 0 ? Executors.newFixedThreadPool(poolSize, threadFactory) :
              Executors.newCachedThreadPool(threadFactory);
      if (servers == null) {
          throw new NullPointerException("servers is null");
      }
      List<InetSocketAddress> addresses = AddrUtil.getAddresses(servers);
      assert addresses != null;
      assert addresses.size() > 0;

      try {
          client = new MemcachedClient(addresses);
          if (client != null) {
              client.addObserver(new ConnectionObserver() {

                  @Override
                  public void connectionEstablished(SocketAddress socketAddress, int i) {
                      closed.set(false);
                  }

                  @Override
                  public void connectionLost(SocketAddress socketAddress) {
                      closed.set(true);
                  }
              });
          }
      }
      catch (IOException e) {
          throw new RuntimeException(e);
      }
      return client;
  }

  @PreDestroy
    private void disconnect() {
        try {
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
                try {
                    executorService.awaitTermination(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    if (!executorService.isShutdown()) {
                        executorService.shutdownNow();
                    }
                }
            }

            if (client != null) {
                client.shutdown();
            }
        }
        catch (Exception e) {
            throw new CacheException(e);
        } finally {
            client = null;
        }
        closed.set(true);
    }

  private static String property(final Properties properties, final String cacheName, final String name, final String defaultValue) {
    return properties.getProperty(cacheName + "." + name, properties.getProperty(name, defaultValue));
  }

  @Override
  public V get(K key) {
      checkState();

      if (key == null) {
          throw new NullPointerException();
      }

      if (configuration.isReadThrough()) {
          cacheLoader.load(key);
      }

      return client.<V>get(key, transcoder);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) {
      checkState();

      if (keys == null || keys.contains(null)) {
          throw new NullPointerException();
      }

      if (configuration.isReadThrough() && cacheLoader != null) {
          cacheLoader.loadAll(keys);
      }

      Map<String, V> values = client.<V>getBulk(keys.stream()
                      .<String>map(key -> key)
                      .collect(Collectors.toSet()),
              transcoder);
      return (Map<K, V>) values;
  }

  @Override
  public boolean containsKey(K key) {
      checkState();

      if (key == null) {
          throw new NullPointerException();
      }

      return client.get(key) != null;
  }

  public void touchKey(K key) {
      checkState();

      if (key == null) {
          throw new NullPointerException();
      }

      client.touch(key, expiry);
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

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    executorService.submit(
        new Runnable() {
          @Override
          public void run() {
            try {
              if (cacheLoader != null) {
                for (K key : keys) {
                  Object value = client.get(key);
                  if (value == null || replaceExistingValues) {
                      cacheLoader.load(key);
                  }
                }
              }
            } catch (Exception e) {
              cl.onException(e);
            } finally {
              cl.onCompletion();
            }
          }
        });
  }

  @Override
  public void put(K key, V value) {
    checkState();

    if (key == null || value == null) {
      throw new NullPointerException();
    }

      OperationFuture<Boolean> of = client.set(key, expiry, value);
      try {
          of.get();
      } catch (InterruptedException | ExecutionException e) {
          of.cancel();
      }
  }

  @Override
  public V getAndPut(K key, V value) {
      checkState();

      if (key == null) {
          throw new NullPointerException();
      }

      if (configuration.isReadThrough()) {
          cacheLoader.load(key);
      }

      // TODO(gburd): Is this really how CAS works for MemcacheD, I doubt it...
      CASValue<V> casValue = client.<V>gets(key, transcoder);
      CASResponse casResponse = client.cas(key, casValue.getCas(), expiry, value, transcoder);
      return casValue.getValue();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    checkState();

    if (map == null || map.containsKey(null) || map.containsValue(null)) {
      throw new NullPointerException();
    }

      Set<OperationFuture<Boolean>> ofs = new HashSet<>(map.size());
      for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
          ofs.add(client.set(entry.getKey(), expiry, entry.getValue()));
      }

      for (OperationFuture<Boolean> of : ofs) {
          try {
              of.get();
          } catch (InterruptedException | ExecutionException e) {
              of.cancel();
          }
      }
  }

  @Override
  public boolean putIfAbsent(K key, V value) {
    checkState();

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    return (view.putIfAbsent(key, value) == null);
  }

  @Override
  public boolean remove(K key) {
    checkState();

    if (key == null) {
      throw new NullPointerException();
    }

      OperationFuture<Boolean> of = client.delete(key);
      try {
          return of.get();
      } catch (InterruptedException | ExecutionException e) {
          of.cancel();
          return false;
      }
  }

  @Override
  public boolean remove(K key, V oldValue) {
    checkState();

    if (key == null || oldValue == null) {
      throw new NullPointerException();
    }

    return view.remove(key, oldValue);
  }

  @Override
  public V getAndRemove(K key) {
    checkState();

    if (key == null) {
      throw new NullPointerException();
    }

    return view.remove(key);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    checkState();

    if (key == null || oldValue == null || newValue == null) {
      throw new NullPointerException();
    }

    return view.replace(key, oldValue, newValue);
  }

  @Override
  public boolean replace(K key, V value) {
    checkState();

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    return (view.replace(key, value) != null);
  }

  @Override
  public V getAndReplace(K key, V value) {
    checkState();

    if (key == null || value == null) {
      throw new NullPointerException();
    }

    return view.replace(key, value);
  }

  @Override
  public void removeAll(Set<? extends K> keys) {
    checkState();

    if (keys == null || keys.contains(null)) {
      throw new NullPointerException();
    }

    cache.invalidateAll(keys);
  }

  @Override
  public void removeAll() {
    checkState();

    cache.invalidateAll();
  }

  @Override
  public void clear() {
    checkState();

    view.clear();
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
    if (closed.compareAndSet(false, true)) {
      cache.invalidateAll();
      cache.cleanUp();

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

  @Override
  public boolean isClosed() {
    return closed.get();
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
    checkState();

    List<javax.cache.Cache.Entry<K, V>> list = new ArrayList<>();

    for (final Map.Entry<K, V> entry : view.entrySet()) {
      list.add(
          new javax.cache.Cache.Entry<K, V>() {
            @Override
            public K getKey() {
              return entry.getKey();
            }

            @Override
            public V getValue() {
              return entry.getValue();
            }

            @Override
            public <T> T unwrap(Class<T> clazz) {
              return clazz.cast(entry);
            }
          });
    }

    return Collections.unmodifiableList(list).iterator();
  }

  @Override
  public void onRemoval(RemovalNotification<K, V> notification) {
    switch (notification.getCause()) {
      case EXPIRED:
        notifyListeners(new MemcachedCacheEntryEvent<>(this, EventType.EXPIRED, notification));
        break;

      case EXPLICIT:
        notifyListeners(new MemcachedCacheEntryEvent<>(this, EventType.REMOVED, notification));
        break;

      case REPLACED:
        notifyListeners(new MemcachedCacheEntryEvent<>(this, EventType.UPDATED, notification));
        break;
    }
  }

  public void cleanUp() {
    cache.cleanUp();
  }

  public long size() {
    return cache.size();
  }

  public CacheStats stats() {
    return cache.stats();
  }

  private void checkState() {
    if (isClosed()) {
      throw new IllegalStateException("This cache is closed!");
    }
  }

  private void notifyListeners(CacheEntryEvent<K, V> event) {
    for (CacheEntryListenerConfiguration<K, V> listenerConfiguration :
        cacheEntryListenerConfigurations) {
      boolean invokeListener = true;

      if (listenerConfiguration.getCacheEntryEventFilterFactory() != null) {
        invokeListener =
            listenerConfiguration.getCacheEntryEventFilterFactory().create().evaluate(event);
      }

      if (invokeListener) {
        CacheEntryListener<?, ?> cel =
            listenerConfiguration.getCacheEntryListenerFactory().create();

        switch (event.getEventType()) {
          case CREATED:
            if (cel instanceof CacheEntryCreatedListener) {
              throw new CacheEntryListenerException("Not supported!");
            }
            break;

          case EXPIRED:
            if (cel instanceof CacheEntryExpiredListener) {
              ((CacheEntryExpiredListener) cel).onExpired(Sets.newHashSet(event));
            }
            break;

          case REMOVED:
            if (cel instanceof CacheEntryRemovedListener) {
              ((CacheEntryRemovedListener) cel).onRemoved(Sets.newHashSet(event));
            }
            break;

          case UPDATED:
            if (cel instanceof CacheEntryUpdatedListener) {
              ((CacheEntryUpdatedListener) cel).onUpdated(Sets.newHashSet(event));
            }
            break;
        }
      }
    }
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
         * <p>
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

}
