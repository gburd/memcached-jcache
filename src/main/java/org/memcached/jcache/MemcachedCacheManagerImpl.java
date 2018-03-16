/*
 * Copyright 2018 Onshape, Inc..
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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.lang3.StringUtils;

public class MemcachedCacheManagerImpl implements MemcachedCacheManager {
  private static final Logger LOG = Logger.getLogger(MemcachedCacheManagerImpl.class.getName());

  private final URI uri;
  private final ClassLoader classLoader;
  private final Properties properties;
  private final MemcachedCachingProvider cachingProvider;
  private final ConcurrentMap<String, MemcachedCache<?, ?>> caches = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, MemcachedClient> clients = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Object lock = new Object();
  private MemcachedClient client;

  public MemcachedCacheManagerImpl(
      URI uri,
      ClassLoader classLoader,
      Properties properties,
      MemcachedCachingProvider cachingProvider) {
    this.uri = uri;
    this.classLoader = classLoader;
    this.properties = properties;
    this.cachingProvider = cachingProvider;
  }

  @Override
  public MemcachedCachingProvider getCachingProvider() {
    return cachingProvider;
  }

  @Override
  public URI getURI() {
    return uri;
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(
      String cacheName, C configuration) throws IllegalArgumentException {
    checkState();

    if (cacheName == null || configuration == null) {
      throw new NullPointerException();
    } else if (!(configuration instanceof CompleteConfiguration)) {
      throw new IllegalArgumentException("Invalid configuration implementation!");
    }

    CompleteConfiguration<K, V> completeConfiguration = (CompleteConfiguration) configuration;

    validateConfiguration(completeConfiguration);

    synchronized (lock) {
      if (caches.containsKey(cacheName)) {
        throw new CacheException("This cache already exists!");
      }

      MemcachedCache<K, V> cache = new MemcachedCache<K, V>(cacheName, completeConfiguration, this);

      caches.put(cacheName, cache);

      return cache;
    }
  }

  @Override
  public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
    checkState();

    if (cacheName != null && caches.containsKey(cacheName)) {
      Cache<?, ?> cache = caches.get(cacheName);

      CompleteConfiguration<?, ?> configuration =
          cache.getConfiguration(CompleteConfiguration.class);

      if (configuration.getKeyType().isAssignableFrom(keyType)
          && configuration.getValueType().isAssignableFrom(valueType)) {
        return (Cache<K, V>) cache;
      }

      throw new IllegalArgumentException(
          "Provided key and/or value types are incompatible with this cache!");
    }

    return null;
  }

  @Override
  public <K, V> Cache<K, V> getCache(String cacheName) {
    return (Cache<K, V>) getCache(cacheName, Object.class, Object.class);
  }

  @Override
  public Iterable<String> getCacheNames() {
    checkState();
    return Collections.unmodifiableSet(new HashSet<>(caches.keySet()));
  }

  @Override
  public void destroyCache(String cacheName) {
    checkState();

    if (cacheName == null) {
      throw new NullPointerException();
    }

    Cache<?, ?> cache = caches.get(cacheName);
    if (cache != null) {
      cache.clear();
      cache.close();
    }
    caches.remove(cacheName);
  }

  @Override
  public void enableManagement(String cacheName, boolean enabled) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void enableStatistics(String cacheName, boolean enabled) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      for (Cache<?, ?> c : caches.values()) {
        try {
          c.close();
        } catch (Exception e) {
          // no-op
        }
      }

      caches.clear();

      ((MemcachedCachingProvider) cachingProvider).close(this);
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz.equals(MemcachedClient.class)) {
        return clazz.cast(clients.get(""));
    } else {
        throw new IllegalArgumentException();
    }
  }

  public synchronized void closeMemcachedClientConnection(String cacheName) {
    if (cacheName != null) {
      boolean closeSharedClient = (StringUtils.isBlank(cacheName));
      if (closeSharedClient) {
        MemcachedClient client = clients.get("");
        if (client != null) {
          for (Map.Entry<String, MemcachedClient> entry : clients.entrySet()) {
            if (entry.getValue() == client) {
              clients.remove(entry.getKey());
            }
          }
          if (client.getConnection().isAlive()) {
            client.shutdown();
          }
        }
        clients.remove("");
      } else {
        MemcachedCache<?, ?> cache = caches.get(cacheName);
        if (cache != null) {
          MemcachedClient client = clients.get(cacheName);
          if (client != null) {
            if (client != clients.get("")) {
              if (client.getConnection().isAlive()) {
                client.shutdown();
              }
            }
          }
          clients.remove(cacheName);
        }
      }
    }
  }

  public synchronized MemcachedClient getMemcachedClient(String cacheName) {
    if (StringUtils.isBlank(cacheName)) {
      return null;
    }
    MemcachedCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      return null;
    }
    MemcachedClient client = clients.get(cacheName);
    if (client != null
        && (client.getConnection().isShutDown() || !client.getConnection().isAlive())) {
      closeMemcachedClientConnection(cacheName);
      client = null;
    }
    boolean sharedClient =
        Boolean.valueOf(properties.getProperty(cacheName + ".useSharedClientConnection", "false"));
    if (sharedClient) {
      client = clients.get("");
    }
    if (client == null) {
      ConnectionFactory connectionFactory = getConnectionFactory();

      String serversPropertyKey = sharedClient ? "servers" : cacheName + ".servers";
      String servers = properties.getProperty(serversPropertyKey);
      List<InetSocketAddress> addresses = null;
      if (StringUtils.isBlank(servers)) {
        LOG.warning(
            String.format(
                "Invalid or missing server addresses in properties (key: %s).",
                serversPropertyKey));
      } else {
        addresses =
            AddrUtil.getAddresses(servers)
                .stream()
                .filter(
                    address ->
                        serviceAvailable(address.getAddress().getHostAddress(), address.getPort()))
                .collect(Collectors.toList());
      }

      if (addresses != null && !addresses.isEmpty()) {
        try {
          client = new MemcachedClient(connectionFactory, addresses);
          client.addObserver(
              new ConnectionObserver() {
                @Override
                public void connectionEstablished(SocketAddress socketAddress, int i) {
                  LOG.info(
                      String.format(
                          "Connection to MemcacheD established: %s on reconnect # %d.",
                          socketAddress.toString(), i));
                }

                @Override
                public void connectionLost(SocketAddress socketAddress) {
                  LOG.info(
                      String.format("Lost connection to MemcacheD: ", socketAddress.toString()));
                }
              });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      if (client != null) {
        if (client.getConnection().isAlive()) {
          if (sharedClient) {
            clients.put("", client);
          }
          clients.put(cacheName, client);
        } else {
          client = null;
        }
      }
    }
    if (client == null) {
      throw new IllegalStateException(
          "Unable to establish a client connection to MemcacheD for "
              + ("".equals(cacheName) ? "shared connection" : cacheName));
    }
    return client;
  }

  protected void close(Cache<?, ?> cache) {
    caches.remove(cache.getName());
  }

  private ConnectionFactory getConnectionFactory() {
    ConnectionFactory connectionFactory = cachingProvider.getConnectionFactory();
    if (connectionFactory == null) {
      connectionFactory = new BinaryConnectionFactory();
    }
    return connectionFactory;
  }

  private static boolean serviceAvailable(String ip, int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(ip, port), 500);
      return true;
    } catch (Exception ex) {
    }
    return false;
  }

  private void checkState() {
    if (isClosed()) {
      throw new IllegalStateException("This cache manager is closed!");
    }
  }

  private void validateConfiguration(CompleteConfiguration<?, ?> configuration) {
    if (configuration.isStoreByValue()) {
      throw new UnsupportedOperationException("Store by value is not supported in Memcached!");
    }

    if (configuration.getExpiryPolicyFactory() == null) {
      throw new NullPointerException("Expiry policy factory cannot be null!");
    }

    ExpiryPolicy expiryPolicy = configuration.getExpiryPolicyFactory().create();

    if (!(expiryPolicy instanceof EternalExpiryPolicy)
        && !(expiryPolicy instanceof ModifiedExpiryPolicy)
        && !(expiryPolicy instanceof TouchedExpiryPolicy)) {
      throw new UnsupportedOperationException("Invalid expiry policy configuration!");
    }

    if (configuration.isReadThrough() && configuration.getCacheLoaderFactory() == null) {
      throw new IllegalArgumentException("Invalid read through cache configuration!");
    }

    if (configuration.isWriteThrough() || configuration.getCacheWriterFactory() != null) {
      throw new UnsupportedOperationException("Invalid write through cache configuration!");
    }
  }
}
