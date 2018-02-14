package org.memcached.jcache;

import com.thimbleware.jmemcached.*;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.BlockStorageCacheStorage;
import com.thimbleware.jmemcached.storage.bytebuffer.ByteBufferBlockStore;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

/** */
public abstract class AbstractMemcachedTest {
  protected static final int MAX_BYTES = 4194304;
  public static final int CEILING_SIZE = 4194304;
  public static final int MAX_SIZE = 1000;
  protected MemCacheDaemon<LocalCacheElement> daemon;
  public int port;
  protected Cache<LocalCacheElement> cache;
  protected final CacheType cacheType;
  protected final int blockSize;
  private final ProtocolMode protocolMode;

  public AbstractMemcachedTest() {
    this.blockSize = 4;
    this.cacheType = CacheType.LOCAL_HASH;
    this.protocolMode = ProtocolMode.BINARY;
  }

  public AbstractMemcachedTest(CacheType cacheType, int blockSize, ProtocolMode protocolMode) {
    this.blockSize = blockSize;
    this.cacheType = cacheType;
    this.protocolMode = protocolMode;
  }

  public static enum CacheType {
    LOCAL_HASH,
    BLOCK,
    MAPPED
  }

  public static enum ProtocolMode {
    TEXT,
    BINARY
  }

  @Parameterized.Parameters
  public static Collection blockSizeValues() {
    return Arrays.asList(
        new Object[][] {
          {CacheType.LOCAL_HASH, 1, ProtocolMode.TEXT},
          {CacheType.LOCAL_HASH, 1, ProtocolMode.BINARY},
          {CacheType.BLOCK, 4, ProtocolMode.TEXT},
          {CacheType.BLOCK, 4, ProtocolMode.BINARY},
          {CacheType.MAPPED, 4, ProtocolMode.TEXT},
          {CacheType.MAPPED, 4, ProtocolMode.BINARY}
        });
  }

  @Before
  public void setup() throws IOException {
    // create daemon and start it
    daemon = new MemCacheDaemon<LocalCacheElement>();
    CacheStorage<Key, LocalCacheElement> cacheStorage = getCacheStorage();

    daemon.setCache(new CacheImpl(cacheStorage));
    daemon.setBinary(protocolMode == ProtocolMode.BINARY);

    port = AvailablePortFinder.getNextAvailable();
    daemon.setAddr(new InetSocketAddress(port));
    daemon.setVerbose(true);
    daemon.start();

    cache = daemon.getCache();
  }

  @After
  public void teardown() {
    if (daemon.isRunning()) daemon.stop();
  }

  private CacheStorage<Key, LocalCacheElement> getCacheStorage() throws IOException {
    CacheStorage<Key, LocalCacheElement> cacheStorage = null;
    switch (cacheType) {
      case LOCAL_HASH:
        cacheStorage =
            ConcurrentLinkedHashMap.create(
                ConcurrentLinkedHashMap.EvictionPolicy.FIFO, MAX_SIZE, MAX_BYTES);
        break;
      case BLOCK:
        cacheStorage =
            new BlockStorageCacheStorage(
                16,
                CEILING_SIZE,
                blockSize,
                MAX_BYTES,
                MAX_SIZE,
                new ByteBufferBlockStore.ByteBufferBlockStoreFactory());
        break;
      case MAPPED:
        cacheStorage =
            new BlockStorageCacheStorage(
                16,
                CEILING_SIZE,
                blockSize,
                MAX_BYTES,
                MAX_SIZE,
                MemoryMappedBlockStore.getFactory());

        break;
    }
    return cacheStorage;
  }

  public MemCacheDaemon getDaemon() {
    return daemon;
  }

  public Cache getCache() {
    return cache;
  }

  public CacheType getCacheType() {
    return cacheType;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public ProtocolMode getProtocolMode() {
    return protocolMode;
  }

  public int getPort() {
    return port;
  }

  /**
   * Finds currently available server ports.
   *
   * @author <a href="http://mina.apache.org">Apache MINA Project</a>
   * @see <a href="http://www.iana.org/assignments/port-numbers">IANA.org</a>
   */
  private static final class AvailablePortFinder {
    /** The minimum number of server port number. */
    public static final int MIN_PORT_NUMBER = 1;

    /** The maximum number of server port number. */
    public static final int MAX_PORT_NUMBER = 49151;

    /** Creates a new instance. */
    private AvailablePortFinder() {
      // Do nothing
    }

    /**
     * Returns the {@link Set} of currently available port numbers ({@link Integer}). This method is
     * identical to <code>getAvailablePorts(MIN_PORT_NUMBER, MAX_PORT_NUMBER)</code>.
     *
     * <p>WARNING: this can take a very long time.
     */
    public static Set<Integer> getAvailablePorts() {
      return getAvailablePorts(MIN_PORT_NUMBER, MAX_PORT_NUMBER);
    }

    /**
     * Gets the next available port starting at the lowest port number.
     *
     * @throws NoSuchElementException if there are no ports available
     */
    public static int getNextAvailable() {
      return getNextAvailable(MIN_PORT_NUMBER);
    }

    /**
     * Gets the next available port starting at a port.
     *
     * @param fromPort the port to scan for availability
     * @throws NoSuchElementException if there are no ports available
     */
    public static int getNextAvailable(int fromPort) {
      if (fromPort < MIN_PORT_NUMBER || fromPort > MAX_PORT_NUMBER) {
        throw new IllegalArgumentException("Invalid start port: " + fromPort);
      }

      for (int i = fromPort; i <= MAX_PORT_NUMBER; i++) {
        if (available(i)) {
          return i;
        }
      }

      throw new NoSuchElementException("Could not find an available port " + "above " + fromPort);
    }

    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port to check for availability
     */
    public static boolean available(int port) {
      if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
        throw new IllegalArgumentException("Invalid start port: " + port);
      }

      ServerSocket ss = null;
      DatagramSocket ds = null;
      try {
        ss = new ServerSocket(port);
        ss.setReuseAddress(true);
        ds = new DatagramSocket(port);
        ds.setReuseAddress(true);
        return true;
      } catch (IOException e) {
        // Do nothing
      } finally {
        if (ds != null) {
          ds.close();
        }

        if (ss != null) {
          try {
            ss.close();
          } catch (IOException e) {
            /* should not be thrown */
          }
        }
      }

      return false;
    }

    /**
     * Returns the {@link Set} of currently avaliable port numbers ({@link Integer}) between the
     * specified port range.
     *
     * @throws IllegalArgumentException if port range is not between {@link #MIN_PORT_NUMBER} and
     *     {@link #MAX_PORT_NUMBER} or <code>fromPort</code> if greater than <code>toPort</code>.
     */
    public static Set<Integer> getAvailablePorts(int fromPort, int toPort) {
      if (fromPort < MIN_PORT_NUMBER || toPort > MAX_PORT_NUMBER || fromPort > toPort) {
        throw new IllegalArgumentException("Invalid port range: " + fromPort + " ~ " + toPort);
      }

      Set<Integer> result = new TreeSet<Integer>();

      for (int i = fromPort; i <= toPort; i++) {
        ServerSocket s = null;

        try {
          s = new ServerSocket(i);
          result.add(new Integer(i));
        } catch (IOException e) {
          // Do nothing
        } finally {
          if (s != null) {
            try {
              s.close();
            } catch (IOException e) {
              /* should not be thrown */
            }
          }
        }
      }

      return result;
    }
  }
}
