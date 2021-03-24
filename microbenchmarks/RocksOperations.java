package rocksdbtest.microbenchmarks;

import org.openjdk.jmh.annotations.*;
import org.rocksdb.RocksDBException;
import rocksdbtest.transaction.RocksDatabase;
import rocksdbtest.transaction.RocksTransaction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Collections.reverseOrder;
import static rocksdbtest.transaction.RocksTransaction.toBytes;

public class RocksOperations {

    static String dbPath;
    final static byte[] value = new byte[0];

    static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    @State(Scope.Thread)
    public static class BenchmarkState {

        public RocksDatabase db;
        public Random random;
        public Integer[] keys;

        public BenchmarkState() {
            try {
                dbPath = TEMP_DIR + "rocks_transaction_testdb";
                if (Files.exists(Paths.get(dbPath))) {
                    Files.walk(Paths.get(dbPath)).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
                }
                db = new RocksDatabase(dbPath);
                random = new Random();
                keys = random.ints(1000).boxed().toArray(Integer[]::new);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    public void putPerformance(BenchmarkState state) {
        long conflicts = evaluate(state, (tx, key) -> {
            try {
                tx.put(toBytes(key), toBytes(key));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        });
//        System.out.println("Conflicts written using PUT: " + conflicts);
    }

    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    public void mergePerformance(BenchmarkState state) {
        long conflicts = evaluate(state, (tx, key) -> {
            try {
                tx.merge(toBytes(key), toBytes(key));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        });
//        System.out.println("Conflicts written using MERGE: " + conflicts);
    }

    private static long evaluate(BenchmarkState state, BiConsumer<RocksTransaction, Integer> operator) {
        Set<Integer> inserted = new HashSet<>();
        long conflicts = 0;
        for (int i = 0; i < 100; i++) {
            try (RocksTransaction tx = new RocksTransaction(state.db)) {
                for (int j = 0; j < 100; j++) {
                    int key = state.random.nextInt(state.keys.length);
                    operator.accept(tx, key);
                    if (inserted.contains(key)) conflicts++;
                    else inserted.add(key);
                }
            }
        }
        return conflicts;
    }

}
