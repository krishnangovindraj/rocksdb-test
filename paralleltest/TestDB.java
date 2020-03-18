package rocksdbtest.paralleltest;

import org.rocksdb.*;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestDB {

    private static final byte[] EMPTY = new byte[]{};
    private static final int CHUNKS = 1000;
    private static final int CHUNK_SIZE = 10_000;

    private static volatile byte[] sink;

    public static void doTest(OptimisticTransactionDB db) throws RocksDBException {
//        writeData(db);
        readDataWithParallelIteratorsInTransaction(db);
        pointGetRandomData(db);
    }

    public static void writeData(OptimisticTransactionDB db) throws RocksDBException {
        bench("writing values", () -> {
            for (int i = 0; i < CHUNKS * CHUNK_SIZE; ++i) {
                db.put(toBytes(i), EMPTY);
            }
        });
    }

    public static void pointGetRandomData(OptimisticTransactionDB db) throws RocksDBException {
        Random random = new Random();

        try (WriteOptions writeOptions = new WriteOptions();
             ReadOptions readOptions = new ReadOptions();
             Transaction tx = db.beginTransaction(writeOptions)) {
            tx.setSnapshot();
            readOptions.setSnapshot(tx.getSnapshot());

            bench("getting random values", () -> IntStream.range(0, CHUNKS).parallel().forEach(i -> {
                for (int j = 0; j < CHUNK_SIZE; ++j) {
                    int x = random.nextInt(CHUNK_SIZE);

                    try {
                        sink = tx.get(readOptions, toBytes((i * CHUNK_SIZE) + x));
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));

            {
                int x = random.nextInt(CHUNKS * CHUNK_SIZE);

                bench("everyone getting the same value", () -> IntStream.range(0, CHUNKS).parallel().forEach(i -> {
                    for (int j = 0; j < CHUNK_SIZE; ++j) {
                        try {
                            sink = tx.get(readOptions, toBytes(x));
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
            }
        }
    }

    public static void readDataWithParallelIteratorsInTransaction(OptimisticTransactionDB db) throws RocksDBException {

        try (WriteOptions writeOptions = new WriteOptions();
             ReadOptions readOptions = new ReadOptions();
             Transaction tx = db.beginTransaction(writeOptions)) {
            tx.setSnapshot();
            readOptions.setSnapshot(tx.getSnapshot());

            {
                List<I> iList = IntStream.range(0, CHUNKS)
                        .mapToObj(i -> new I(tx.getIterator(readOptions), i))
                        .collect(Collectors.toList());

                bench("iterating values striped", () -> iList.parallelStream().forEach(TestDB::iterate));
            }

            {
                Random random = new Random();
                List<I> iList = IntStream.range(0, CHUNKS)
                        .mapToObj(i -> new I(tx.getIterator(readOptions), random.nextInt(CHUNKS)))
                        .collect(Collectors.toList());

                bench("iterating values random chunks", () -> iList.parallelStream().forEach(TestDB::iterate));
            }

            {
                Random random = new Random();
                int chunk = random.nextInt(CHUNKS);
                List<I> iList = IntStream.range(0, CHUNKS)
                        .mapToObj(i -> new I(tx.getIterator(readOptions), chunk))
                        .collect(Collectors.toList());

                bench("everyone iterating one chunk", () -> iList.parallelStream().forEach(TestDB::iterate));
            }

            tx.commit();
        }
    }

    private static void iterate(I i) {
        i.iter.seek(toBytes(i.i * CHUNK_SIZE));
        for (int j = 0; j < CHUNK_SIZE; ++j) {

//                    System.out.println(Thread.currentThread() + ": "
//                            + new String(i.iter.key()) + ": "
//                            + new String(i.iter.value()));
//                    loaded.put(new String(i.iter.key()), new String(i.iter.value()));
//                    byteMap[(i.i * CHUNK_SIZE) + j] = new byte[][] {i.iter.key().clone(), i.iter.value().clone()};
            sink = i.iter.key();
            sink = i.iter.value();
            i.iter.next();
        }
    }

    private static class I {
        private final RocksIterator iter;
        private final int i;

        private I(RocksIterator iter, int i) {
            this.iter = iter;
            this.i = i;
        }
    }

    private static void bench(String name, RocksCode code) throws RocksDBException {
        System.out.println("Started " + name);
        long startMillis = System.currentTimeMillis();
        code.run();
        long endMillis = System.currentTimeMillis();
        System.out.println("Finished " + name + ", took: " + (endMillis - startMillis) + "ms");
    }

    @FunctionalInterface
    private static interface RocksCode {
        void run() throws RocksDBException;
    }

    public static void main(String[] args) {
        String tempDir = System.getProperty("java.io.tmpdir");

        String dbPath = tempDir + "rocks_iterator_parralel_testdb";

        System.out.println("DB path is: " + dbPath);

        try (final Options options = new Options()) {
            options.setCreateIfMissing(true);

            try (final OptimisticTransactionDB db = OptimisticTransactionDB.open(options, dbPath)) {
                doTest(db);
            } catch (RocksDBException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static byte[] toBytes(final int data) {
        return new byte[] {
                (byte)(data >> 24),
                (byte)(data >> 16),
                (byte)(data >> 8),
                (byte) data,
        };
    }

    private int toInt(byte[] data) {
        if (data == null || data.length != 4) return 0x0;
        // ----------
        return  data[0] << 24  |
                data[1] << 16  |
                data[2] << 8   |
                data[3];
    }
}
