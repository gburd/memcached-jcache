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

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.OperationsException;

import org.apache.commons.lang3.StringUtils;

import com.diffplug.common.base.Errors;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

@SuppressWarnings("rawtypes")
public class MemcachedCache<K, V> implements javax.cache.Cache<K, V> {
    private static final Logger LOG = Logger.getLogger(MemcachedCache.class.getName());

    private final String cacheName;
    private final CompleteConfiguration<K, V> configuration;
    private final ExpiryPolicy expiryPolicy;
    private final MemcachedCacheManagerImpl cacheManager;
    private final Statistics statistics = new Statistics();
    private final MemcachedCacheLoader cacheLoader;
    private final AtomicBoolean closed;
    private final Transcoder<V> transcoder;
    private MemcachedKeyCodec keyCodec = null;

    public MemcachedCache(String cacheName, CompleteConfiguration<K, V> configuration, CacheManager cacheManager) {
        this.cacheName = cacheName;
        this.configuration = configuration;
        this.cacheManager = (MemcachedCacheManagerImpl) cacheManager;

        if (configuration.isManagementEnabled()) {
            MemcachedCacheMXBean bean = new MemcachedCacheMXBean(this);

            try {
                ManagementFactory.getPlatformMBeanServer()
                    .registerMBean(bean, new ObjectName(bean.getObjectName()));
            }
            catch (OperationsException | MBeanException e) {
                throw new CacheException(e);
            }
        }

        expiryPolicy = configuration.getExpiryPolicyFactory().create();

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
                    .registerMBean(bean, new ObjectName(MemcachedCacheStatisticsMXBean.getObjectName(this)));
            }
            catch (OperationsException | MBeanException e) {
                throw new CacheException(e);
            }
        }

        MemcachedCachingProvider provider = (MemcachedCachingProvider) cacheManager.getCachingProvider();
        transcoder = getTranscoder(provider.getConnectionFactory());

        // The 'closed' state is logical, meaning "we're not going to re-open or use the client
        // connection to MemcacheD. This is different from the client being null or the client
        // not able to connect to MemcacheD, in those cases we will keep trying to reconnect in
        // hopes that the failure is transient.
        closed = new AtomicBoolean(false);
    }

    private String property(
            final Properties properties,
            final String cacheName,
            final String name,
            final String defaultValue) {
        return properties.getProperty(
                cacheName + "." + name, properties.getProperty(name, defaultValue));
    }

    private MemcachedKeyCodec getKeyCodec() {
        if (keyCodec == null) {
            Properties properties = cacheManager.getProperties();
            String keyCodecClassName = properties.getProperty("KeyCodec", DefaultKeyCodec.class.getCanonicalName());
            try {
                Class<?> c = Class.forName(keyCodecClassName);
                Constructor<?> cons = c.getConstructor();
                keyCodec = (MemcachedKeyCodec) cons.newInstance();
            }
            catch (IllegalAccessException
                    | InstantiationException
                    | InvocationTargetException
                    | NoSuchMethodException
                    | ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        }
        if (keyCodec == null) {
            keyCodec = new DefaultKeyCodec();
        }
        return keyCodec;
    }

    private String encodedKeyFor(Object key) {
        MemcachedKeyCodec keyCodec = getKeyCodec();
        String keyString = key.toString();
        if (keyCodec != null) {
            try {
                return keyCodec.encode(cacheName, key);
            }
            catch (Throwable t) {
                LOG.warning("Custom key encoder failed. " + t.getStackTrace());
                return keyString;
            }
        } else {
            if (!keyString.startsWith(cacheName)) {
                return String.format("%s.%s", cacheName, keyString);
            }
        }
        return keyString;
    }

    private Object decodeKeyFor(String key) {
        MemcachedKeyCodec keyCodec = getKeyCodec();
        return (keyCodec != null)
                ? keyCodec.decode(cacheName, key)
                : key.substring(cacheName.length() + 1);
    }

    private int getExpiryForAccess() {
        Duration duration = expiryPolicy.getExpiryForAccess();
        return (duration == null || duration.isEternal())
                ? 0
                : Math.toIntExact(duration.getDurationAmount());
    }

    private int getExpiryForUpdate() {
        Duration duration = expiryPolicy.getExpiryForUpdate();
        return (duration == null || duration.isEternal())
                ? 0
                : Math.toIntExact(duration.getDurationAmount());
    }

    private int getExpiryForCreation() {
        Duration duration = expiryPolicy.getExpiryForCreation();
        return (duration == null || duration.isEternal())
                ? 0
                : Math.toIntExact(duration.getDurationAmount());
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        MemcachedClient client = checkState();

        V value = asCompletableFuture(client.asyncGet(encodedKeyFor(key), transcoder))
                .exceptionally(
                        Errors.rethrow()
                                .wrapFunction(
                                        e -> {
                                            throw new CacheException(e);
                                        }))
                .join();

        if (statisticsEnabled) {
            if (value != null) {
                statistics.increaseHits(1);
            } else {
                statistics.increaseMisses(1);
            }
        }

        if (value == null && cacheLoader != null && configuration.isReadThrough()) {
            timer.stop();
            value = (V) cacheLoader.load(key);
            if (value != null) {
                final int seconds = getExpiryForAccess();
                asCompletableFuture(client.set(encodedKeyFor(key), seconds, value, transcoder))
                        .exceptionally(
                                Errors.rethrow()
                                        .wrapFunction(
                                                e -> {
                                                    throw new CacheException(e);
                                                }))
                        .join();
            }
        }

        if (statisticsEnabled) {
            statistics.addGetTime(timer.elapsed());
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        if (keys == null || keys.size() == 0) {
            return Collections.<K, V> emptyMap();
        }

        MemcachedClient client = checkState();
        Map<Object, V> kvp = asCompletableFuture(client.<V> asyncGetBulk(
                keys.stream()
                        .filter(Objects::nonNull)
                        .<String> map(key -> encodedKeyFor(key)).collect(Collectors.toSet()), transcoder))
                .exceptionally(
                        Errors.rethrow()
                                .wrapFunction(
                                        e -> {
                                            throw new CacheException(e);
                                        }))
                .join()
                .entrySet()
                .stream()
                .filter(entry -> {
                    if (entry.getKey() == null || entry.getValue() == null) {
                        return false;
                    }
                    return true;
                })
                .collect(Collectors.toMap(entry -> decodeKeyFor(entry.getKey()), Map.Entry::getValue));

        if (configuration.isReadThrough() && cacheLoader != null) {
            Map<String, V> lkvp = cacheLoader.loadAll(keys.stream()
                    .filter(key -> !kvp.containsKey(key))
                    .collect(Collectors.toSet()));
            final int seconds = getExpiryForAccess();
            lkvp.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .map(entry -> asCompletableFuture(
                                client.set(encodedKeyFor(entry.getKey()), seconds, entry.getValue(), transcoder)))
                .forEach(cf -> {
                            cf.exceptionally(
                                    Errors.rethrow()
                                        .wrapFunction(
                                                e -> {
                                                    throw new CacheException(e);
                                                }))
                                .join();
                        });
            kvp.putAll(lkvp);
        }

        return (Map<K, V>) kvp;
    }

    @Override
    public boolean containsKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        MemcachedClient client = checkState();
        V value = client.<V> get(encodedKeyFor(key), transcoder);
        return value != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void loadAll(
            final Set<? extends K> keys,
            final boolean replaceExistingValues,
            final CompletionListener cl) {
        if (keys == null || cl == null) {
            throw new NullPointerException();
        }

        final int seconds = getExpiryForCreation();
        MemcachedClient client = checkState();
        keys.stream()
            .forEach(
                    key -> {
                        try {
                            Object value = client.get(encodedKeyFor(key));
                            if (value == null || replaceExistingValues) {
                                if (cacheLoader != null) {
                                    V loadedValue = (V) cacheLoader.load(key);
                                    if (loadedValue != null) {
                                        client.add(encodedKeyFor(key), seconds, loadedValue, transcoder);
                                    }
                                }
                            }
                        }
                        catch (Exception e) {
                            cl.onException(e);
                        } finally {
                            cl.onCompletion();
                        }
                    });
    }

    @Override
    public void put(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        final int seconds = getExpiryForCreation();
        MemcachedClient client = checkState();
        asCompletableFuture(client.set(encodedKeyFor(key), seconds, value))
            .exceptionally(
                    Errors.rethrow()
                        .wrapFunction(
                                e -> {
                                    throw new CacheException(e);
                                }))
            .join();
        if (statisticsEnabled) {
            statistics.increasePuts(1);
            statistics.addGetTime(timer.elapsed());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getAndPut(K key, V value) {
        if (key == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        final int seconds = getExpiryForUpdate();
        MemcachedClient client = checkState();

        V previousValue = client.<V> get(encodedKeyFor(key), transcoder);
        if (previousValue == null && cacheLoader != null && configuration.isReadThrough()) {
            previousValue = (V) cacheLoader.load(key);
            // NOTE: skip setting the value here, we're about to change it
            // client.set(encodedKeyFor(key), seconds, previousValue, transcoder);
        }
        if (previousValue != null) {
            statistics.increaseHits(1);
        }

        asCompletableFuture(client.set(encodedKeyFor(key), seconds, value, transcoder))
            .exceptionally(
                    Errors.rethrow()
                        .wrapFunction(
                                e -> {
                                    throw new CacheException(e);
                                }))
            .join();

        if (statisticsEnabled) {
            timer.stop();
            statistics.increasePuts(1);
            statistics.addPutTime(timer.elapsed());
        }

        return previousValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        if (map == null || map.containsKey(null) || map.containsValue(null)) {
            throw new NullPointerException();
        }

        final int seconds = getExpiryForCreation();
        MemcachedClient client = checkState();
        map.entrySet()
            .stream()
            .map(entry -> asCompletableFuture(client.set(encodedKeyFor(entry.getKey()), seconds, entry.getValue())))
            .forEach(cf -> {
                        cf.exceptionally(
                                Errors.rethrow()
                                    .wrapFunction(
                                            e -> {
                                                throw new CacheException(e);
                                            }))
                            .join();
                    });
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        final int seconds = getExpiryForCreation();
        MemcachedClient client = checkState();
        // NOTE: 'add()' has the semantics of 'putIfAbsent()' the docs read:
        // "Add an object to the cache iff it does not exist already." (so, bad name but correct albeit
        // unintuitive API call here).
        boolean applied = asCompletableFuture(client.add(encodedKeyFor(key), seconds, value, transcoder))
            .exceptionally(
                    Errors.rethrow()
                        .wrapFunction(
                                e -> {
                                    throw new CacheException(e);
                                }))
            .join();

        if (statisticsEnabled) {
            statistics.increasePuts(1);
            statistics.addPutTime(timer.elapsed());
        }

        return applied;
    }

    @Override
    public boolean remove(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        MemcachedClient client = checkState();
        try {
            if (this.asCompletableFuture(client.delete(encodedKeyFor(key)))
                .exceptionally(
                        Errors.rethrow()
                            .wrapFunction(
                                    e -> {
                                        throw new CacheException(e);
                                    }))
                    .get()) {
                if (statisticsEnabled) {
                    statistics.increaseRemovals(1);
                    statistics.addRemoveTime(timer.elapsed());
                }
                return true;
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheException(e);
        }
        return false;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        if (key == null || oldValue == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        MemcachedClient client = checkState();

        final boolean applied;
        String qualifiedKey = encodedKeyFor(key);
        CASValue<V> casv = client.gets(qualifiedKey, transcoder);
        if (casv != null && oldValue.equals(casv.getValue())) {
            applied = asCompletableFuture(client.delete(qualifiedKey, casv.getCas()))
                .exceptionally(
                        e -> {
                            LOG.warning(
                                    "Attempted cas operation for delete in MemcacheD failed. "
                                            + e.getStackTrace());
                            return false;
                        })
                .join();
        } else {
            applied = false;
        }

        if (statisticsEnabled && applied) {
            statistics.increaseRemovals(1);
            statistics.addRemoveTime(timer.elapsed());
        }

        return applied;
    }

    @Override
    public V getAndRemove(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        MemcachedClient client = checkState();
        CASValue<V> casv = client.gets(encodedKeyFor(key), transcoder);
        V value = null;
        if (casv != null) {
            if (asCompletableFuture(client.delete(encodedKeyFor(key), casv.getCas()))
                .exceptionally(
                        e -> {
                            LOG.info(
                                    String.format(
                                            "Unable to remove key %s from cache %s.", key.toString(), cacheName));
                            return false;
                        })
                .join()) {
                // applied == true
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
        if (key == null || oldValue == null || newValue == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        final int seconds = getExpiryForUpdate();
        MemcachedClient client = checkState();

        boolean applied = false;
        CASValue<V> casv = client.gets(encodedKeyFor(key), transcoder);

        if (casv != null && oldValue.equals(casv.getValue())) {
            timer.reset();
            CASResponse casr = client.cas(encodedKeyFor(key), casv.getCas(), seconds, newValue, transcoder);
            applied = (casr == CASResponse.OK);
            if (!applied) {
                LOG.info(
                        String.format("Unable to replace key %s from cache %s.", key.toString(), cacheName));
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
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        final int seconds = getExpiryForUpdate();
        MemcachedClient client = checkState();
        boolean applied = asCompletableFuture(client.replace(encodedKeyFor(key), seconds, value, transcoder))
            .exceptionally(
                    Errors.rethrow()
                        .wrapFunction(
                                e -> {
                                    throw new CacheException(e);
                                }))
            .join();

        if (statisticsEnabled && applied) {
            statistics.increaseRemovals(1);
            statistics.addRemoveTime(timer.elapsed());
        }
        return applied;
    }

    @Override
    public V getAndReplace(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        final boolean statisticsEnabled = configuration.isStatisticsEnabled();
        final Timer timer = Timer.start(!statisticsEnabled);
        final int seconds = getExpiryForUpdate();
        MemcachedClient client = checkState();
        boolean applied = false;

        CASValue<V> casv = client.gets(encodedKeyFor(key), transcoder);
        if (casv != null && casv.getValue() != null) {
            applied = client.cas(encodedKeyFor(key), casv.getCas(), seconds, value, transcoder) == CASResponse.OK;
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
        if (keys == null) {
            throw new NullPointerException();
        }

        if (keys.size() > 0) {
            MemcachedClient client = checkState();
            keys.stream()
                .map(key -> asCompletableFuture(client.delete(encodedKeyFor(key))))
                .forEach(
                        cf -> {
                            cf.exceptionally(
                                    Errors.rethrow()
                                        .wrapFunction(
                                                e -> {
                                                    throw new CacheException(e);
                                                }))
                                .join();
                        });
        }
    }

    @Override
    public void removeAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        MemcachedClient client = checkState();
        client.flush();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return (C) configuration;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        if (key == null || entryProcessor == null) {
            throw new NullPointerException();
        }

        checkState();
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

                cacheManager.closeMemcachedClientConnection(cacheName);
                cacheManager.close(this);

                if (configuration.isManagementEnabled()) {
                    String name = MemcachedCacheMXBean.getObjectName(this);

                    try {
                        ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(name));
                    }
                    catch (OperationsException | MBeanException e) {
                        throw new CacheException(e);
                    }
                }

                if (configuration.isStatisticsEnabled()) {
                    String name = MemcachedCacheStatisticsMXBean.getObjectName(this);

                    try {
                        ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(name));
                    }
                    catch (OperationsException | MBeanException e) {
                        throw new CacheException(e);
                    }
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
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    protected Transcoder<V> getTranscoder(ConnectionFactory connectionFactory) {
        Transcoder<V> transcoder = (Transcoder<V>) connectionFactory.getDefaultTranscoder();
        try {
            String className = property(cacheManager.getProperties(), "serializer", cacheName, "");
            if (!StringUtils.isBlank(className)) {
                try {
                    transcoder = Transcoder.class.cast(this.getClass().getClassLoader().loadClass(className).newInstance());
                }
                catch (ClassNotFoundException e) {
                    LOG.warning(
                            String.format(
                                    "Unable to load transcoder class name %s for class %s. %s",
                                    className, cacheName, e));
                    throw e;
                }
            }
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new IllegalArgumentException(e);
        }
        return transcoder;
    }

    private synchronized MemcachedClient checkState() {
        if (isClosed()) {
            throw new IllegalStateException("This cache is closed!");
        }
        return cacheManager.getMemcachedClient(cacheName);
    }

    private <T> CompletableFuture<T> asCompletableFuture(Future<T> future) {
        long timeout = Long.valueOf(cacheManager.getProperties().getProperty("requestTimeoutMillis", "2500"));
        return asCompletableFuture(future, timeout, TimeUnit.MILLISECONDS);
    }

    private <T> CompletableFuture<T> asCompletableFuture(
            Future<T> future, long timeout, TimeUnit units) {
        return CompletableFuture.supplyAsync(
                Errors.rethrow()
                    .wrap(
                            () -> {
                                try {
                                    return future.get(timeout, units);
                                }
                                catch (InterruptedException | ExecutionException e) {
                                    throw new RuntimeException(e);
                                }
                            }));
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

        public long elapsed() {
            if (started > 0) {
                long now = stopped == -1 ? System.nanoTime() / 1000 : stopped;
                return now - started;
            }
            return 0;
        }
    }
}
