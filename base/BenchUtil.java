package rocksdbtest.base;

import org.rocksdb.RocksDBException;

public class BenchUtil {
    private static void bench(String name, RocksCode code) throws RocksDBException {
        System.out.println("Started " + name);
        long startMillis = System.currentTimeMillis();
        code.run();
        long endMillis = System.currentTimeMillis();
        System.out.println("Finished " + name + ", took: " + (endMillis - startMillis) + "ms");
    }

    @FunctionalInterface
    public interface RocksCode {
        void run() throws RocksDBException;
    }
}
