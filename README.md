memcached-jcache
================

This is an implementation of the API and SPI from JSR-107 (aka JCache) for MemcacheD using the Spymemcached Java client.

[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

## Usage

## Example - Simple Cache

```java
MutableConfiguration<String, Integer> configuration = new MutableConfiguration<>();
configuration.setStoreByValue(false);
configuration.setTypes(String.class, Integer.class);

CachingProvider cachingProvider = Caching.getCachingProvider(MemcachedCachingProvider.class.getName());
CacheManager cacheManager = cachingProvider.getCacheManager();
Cache<String, Integer> cache = cacheManager.createCache("cache", configuration);

cache.put("key", 1);
Integer value = cache.get("key");
```

## Example - Loading Cache

```java
final CacheLoader<String, Integer> cacheLoader = new CacheLoader<String, Integer>()
{
    @Override
    public Integer load(String key)
        throws CacheLoaderException
    {
        // in a real application the value would probably come from a database...
        return Integer.valueOf(key);
    }

    @Override
    public Map<String, Integer> loadAll(Iterable<? extends String> keys)
        throws CacheLoaderException
    {
        Map<String, Integer> map = new HashMap<>();
        for (String key : keys)
        {
            // in a real application the value would probably come from a database...
            map.put(key, Integer.valueOf(key));
        }
        return map;
    }
};

MutableConfiguration<String, Integer> configuration = new MutableConfiguration<>();
configuration.setStoreByValue(false);
configuration.setTypes(String.class, Integer.class);
custom.setReadThrough(true);
custom.setCacheLoaderFactory
(
    new Factory<CacheLoader<String, Integer>>()
    {
        @Override
        public CacheLoader<String, Integer> create()
        {
            return cacheLoader;
        }
    }
);

CachingProvider cachingProvider = Caching.getCachingProvider(MemcachedCachingProvider.class.getName());
CacheManager cacheManager = cachingProvider.getCacheManager();
Cache<String, Integer> cache = cacheManager.createCache("cache", configuration);

Integer value = cache.get("key");
```

## Known Issues:

* ExpiryPolicy - https://github.com/onshape/memcached-jcache/issues/1

## Documentation

[javax.cache (JSR107 API and SPI 1.0.0 API)](http://ignite.apache.org/jcache/1.0.0/javadoc/javax/cache/package-summary.html)

## Thanks!
This code was derived by starting with a JCache provider based on Google Guava cache: https://github.com/ocafebabe/guava-jcache
