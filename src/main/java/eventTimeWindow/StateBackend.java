package eventTimeWindow;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class StateBackend {

    private final RocksDB rocksDB;
    private final Set<String> prefetchedKeys = ConcurrentHashMap.newKeySet();
    private final LRUCache blockCache;
    private final Statistics statistics;
    private static final Logger logger = LoggerFactory.getLogger(StateBackend.class);
    private final ExecutorService prefetchExecutor = Executors.newFixedThreadPool(3);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    static {
        RocksDB.loadLibrary();
    }

    public StateBackend(String path) throws RocksDBException {
        this.statistics = new Statistics();
        /*
        设置缓存为 256 MB
         */
        this.blockCache = new LRUCache(256 * 1024 * 1024);
        // this.blockCache = new LRUCache(0);
        /*
        配置表选项（设置缓存）
         */
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig().setBlockCache(blockCache).setBlockSize(16 * 1024);
        /*
        配置数据库选项
         */
        Options options = new Options().setCreateIfMissing(true).setTableFormatConfig(tableConfig).setStatistics(statistics);

        this.rocksDB = RocksDB.open(options, path);
    }

    /*
    写入状态
     */
    public void putState(String key, byte[] value) throws RocksDBException {
        rocksDB.put(key.getBytes(), value);
    }

    /*
    批量写入状态
     */
    public void multiPut(List<byte[]> keys, List<byte[]> values) throws RocksDBException {
        for (int i = 0; i < keys.size(); i++) {
            rocksDB.put(keys.get(i), values.get(i));
        }
    }

    /*
    读取状态
     */
    public byte[] getState(String key) throws RocksDBException, InterruptedException {
        if (prefetchedKeys.contains(key)) {
            /*
            已预取
             */
            return rocksDB.get(key.getBytes());
        }
        /*
        模拟磁盘读取延迟
         */
        // Thread.sleep(50);
        return rocksDB.get(key.getBytes());
    }

    /*
    批量读
    Note: Only the void-returning methods perform parallel IO.
     */
    public List<byte[]> multiGet(List<byte[]> keys) throws RocksDBException {
        return rocksDB.multiGetAsList(new ReadOptions(), keys);
    }

    /*
    预取状态
     */
    public void prefetch(Collection<String> keys) {
        if (closed.get())
            return;

        /*
        异步预取状态
         */
        prefetchExecutor.submit(() -> {
            try (ReadOptions readOptions = new ReadOptions()) {
                for (String key : keys) {
                    byte[] value = rocksDB.get(readOptions, key.getBytes());
                    if (value != null) {
                        /*
                        预取出状态
                         */
                        rocksDB.get(readOptions, key.getBytes());
                        prefetchedKeys.add(key);
                        logger.info("Prefetched key: {}", key);
                    }
                }
            } catch (RocksDBException e) {
                logger.warn("Error prefetching state", e);
                throw new RuntimeException(e);
            }
        });
    }

    /*
    清空预取列表
     */
    public void clearPrefetch() {
        prefetchedKeys.clear();
    }

    /*
    获取缓存命中率
     */
    public double getCacheHitRate() {
        long hits = statistics.getTickerCount(TickerType.BLOCK_CACHE_HIT);
        long misses = statistics.getTickerCount(TickerType.BLOCK_CACHE_MISS);

        if (hits + misses == 0) {
            return 0.0;
        }
        return (hits * 100.0) / (hits + misses);
    }

    /*
    关闭
     */
    public void close(){
        closed.set(true);
        prefetchExecutor.shutdown();
        blockCache.close();
        statistics.close();
        rocksDB.close();
    }
}
