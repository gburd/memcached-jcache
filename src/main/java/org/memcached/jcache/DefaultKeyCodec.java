package org.memcached.jcache;

public class DefaultKeyCodec implements MemcachedKeyCodec {

  @Override
  public String encode(String cacheName, Object key) {
    if (!key.toString().startsWith(cacheName)) {
      return String.format("%s.%s", cacheName, key.toString());
    }
    return key.toString();
  }

  @Override
  public Object decode(String cacheName, String key) {
      if (key.startsWith(cacheName)) {
          return key.substring(cacheName.length() + 1);
      } else {
          return key;
      }
  }
}
