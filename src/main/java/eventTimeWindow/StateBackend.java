package eventTimeWindow;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class StateBackend {

    private final RocksDB rocksDB;
    private final Set<String> prefetchedKeys = ConcurrentHashMap.newKeySet();
    private final LRUCache blockCache;
    private static final Logger logger = LoggerFactory.getLogger(StateBackend.class);
    private final ExecutorService prefetchExecutor = Executors.newFixedThreadPool(2);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    static {
        RocksDB.loadLibrary();
    }

    public StateBackend(String path) throws RocksDBException {
        /*
        设置缓存为 256 MB
         */
        this.blockCache = new LRUCache(256 * 1024 * 1024);
        /*
        配置表选项（设置缓存）
         */
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig().setBlockCache(blockCache).setBlockSize(16 * 1024);
        /*
        配置数据库选项
         */
        Options options = new Options().setCreateIfMissing(true).setTableFormatConfig(tableConfig);

        this.rocksDB = RocksDB.open(options, path);
    }

    /*
    写入状态
     */
    public void putState(String key, byte[] value) throws RocksDBException {
        rocksDB.put(key.getBytes(), value);
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
        Thread.sleep(50);
        return rocksDB.get(key.getBytes());
    }

    /*
    预取状态
     */
    public void prefetch(Collection<String> keys) {
        if (closed.get())
            return;

        prefetchExecutor.submit(() -> {
            try (ReadOptions readOptions = new ReadOptions()) {
                for (String key : keys) {
                    byte[] value = rocksDB.get(readOptions, key.getBytes());
                    if (value != null) {
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
    关闭
     */
    public void close(){
        closed.set(true);
        prefetchExecutor.shutdown();
        blockCache.close();
        rocksDB.close();
    }
}
