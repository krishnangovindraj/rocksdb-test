package rocksdbtest.base;

import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Random;

/**
 * A simple key-centric edge-list testing DB. It stores byte array keys in the form `int,int`,
 * where the ints are 4-byte MSB first. This gives us 8-byte keys.
 *
 * The database is set up to optimize use of prefix-iterating, where the prefix is the first int of the pair.
 */
public class KeyTestDB implements AutoCloseable {

    private static final byte[] EMPTY = new byte[]{};

    private final Options options;
    private final OptimisticTransactionDB db;

    private final WriteOptions standardWriteOptions;
    private final ReadOptions standardReadOptions;

    public KeyTestDB() throws RocksDBException {
        String tempDir = System.getProperty("java.io.tmpdir");

        String dbPath = tempDir + "rocksdbtest";

        deleteDirectory(Paths.get(dbPath).toFile());

        System.out.println("DB path is: " + dbPath);

        try {
            options = new Options();
            options.setCreateIfMissing(true);

            options.useFixedLengthPrefixExtractor(4); // single-int prefix iterating

            options.setMemTableConfig(
                    new HashSkipListMemTableConfig()
            );

            options.setTableFormatConfig(
                    new PlainTableConfig()
                            .setKeySize(8)
            );

            options.setAllowConcurrentMemtableWrite(false);

            db = OptimisticTransactionDB.open(options, dbPath);

            standardWriteOptions = new WriteOptions();
            standardReadOptions = new ReadOptions();
            standardReadOptions.setPrefixSameAsStart(true);
        } catch (RuntimeException ex) {
            try {
                close();
            } catch (RuntimeException ex2) {
                ex2.addSuppressed(ex);
                throw ex2;
            }
            throw ex;
        }
    }

    public void addRandomNodes(final int seed,
                               final int nodeNumber,
                               final int minEdges,
                               final int maxEdges
    ) throws RocksDBException{
        final Random random = new Random(seed);
        for (int i = 0; i < nodeNumber; ++i) {
            final int x = random.nextInt(nodeNumber);
            final int edgeNumber = random.nextInt(maxEdges - minEdges) + minEdges;
            for (int j = 0; j < edgeNumber; ++j) {
                final int y = random.nextInt(nodeNumber);
                db.put(makeKey(x, y), EMPTY);
            }
        }
    }

    public RocksIterator iterateKey(int prefix) {
        RocksIterator iterator = db.newIterator(standardReadOptions);
        try {
            iterator.seek(makePrefixKey(prefix));
            return iterator;
        } catch (RuntimeException ex) {
            iterator.close();
            throw ex;
        }
    }

    public static byte[] makeKey(int first, int second) {
        return ByteBuffer.allocate(Integer.BYTES * 2).putInt(first).putInt(second).array();
    }

    public static byte[] makePrefixKey(int prefix) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(prefix).array();
    }

    public static int getPrefix(byte[] key) {
        return ByteBuffer.wrap(key).getInt();
    }

    public static int getEnd(byte[] key) {
        return ByteBuffer.wrap(key).getInt(4);
    }

    public static String keyString(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key);
        return bb.getInt() + ":" + bb.getInt();
    }


//    public class Transaction implements AutoCloseable {
//        private final org.rocksdb.Transaction tx;
//
//        Transaction() {
//            tx = db.beginTransaction(standardWriteOptions);
//        }
//
//        public RocksIterator iterateKey(int first) {
//            return tx.getIterator(standardReadOptions);
//        }
//
//        @Override
//        public void close() throws Exception {
//            tx.close();
//        }
//    }

    @Override
    public void close() {
        if (standardReadOptions != null) standardReadOptions.close();
        if (standardWriteOptions != null) standardWriteOptions.close();
        if (db != null) db.close();
        if (options != null) options.close();
    }

    private static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
