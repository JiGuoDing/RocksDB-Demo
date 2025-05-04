package rocksDBDemo;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBDemo {

    static {
        /*
        0. 初始化 RocksDB 库，由于 RocksDB 是 C++ 写的，因此在 Java 中使用需要加载动态库
         */
        RocksDB.loadLibrary();
    }

    private static final Logger logger = LoggerFactory.getLogger(RocksDBDemo.class);

    public static void main(String[] args) throws RocksDBException {
        /*
        1. 设置 RocksDB 的配置选项
         */
        Options options = new Options();
        // 如果数据库不存在，则自动创建
        options.setCreateIfMissing(true);
        // 设置数据存储路径
        RocksDB rocksDB = RocksDB.open(options, "data/rocksdb");

        /*
        2. 写入数据
         */
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();
        rocksDB.put(key, value);

        /*
        3. 读取数据
         */
        logger.info("key = {}, value = {}", new String(key), new String(value));

        /*
        关闭数据库
         */
        rocksDB.close();
        options.close();
    }
}
