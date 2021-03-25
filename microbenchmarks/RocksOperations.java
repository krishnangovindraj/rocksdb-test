package rocksdbtest.microbenchmarks;

import org.openjdk.jmh.annotations.*;
import org.rocksdb.RocksDBException;
import rocksdbtest.transaction.RocksDatabase;
import rocksdbtest.transaction.RocksTransaction;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        public Integer[] noiseKeys;
        public Integer[] mergeKeys;
        public Integer[] putKeys;
        public ExecutorService threadPool;

        public BenchmarkState() {
            try {
                dbPath = TEMP_DIR + "rocks_transaction_testdb";
                if (Files.exists(Paths.get(dbPath))) {
                    Files.walk(Paths.get(dbPath)).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
                }
                db = new RocksDatabase(dbPath);
                random = new Random();
                noiseKeys = random.ints(10_000_000).boxed().toArray(Integer[]::new);
                putKeys = random.ints(10_000_000).boxed().toArray(Integer[]::new);
                mergeKeys = random.ints(1).boxed().toArray(Integer[]::new);
                threadPool = Executors.newFixedThreadPool(10);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    public void putPerformance(BenchmarkState state) {
        evaluate(state, state.putKeys, (tx, key) -> {
            try {
                tx.putUntracked(toBytes(key), toBytes(1));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        });
        System.out.println("[low-collision] PUT benchmark is done, sampling from " + state.putKeys.length + " possible keys to put.");
    }

    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    public void mergePerformance(BenchmarkState state) {
       evaluate(state, state.mergeKeys, (tx, key) -> {
            try {
                tx.mergeUntracked(toBytes(key), toBytes(1));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        });
        System.out.println("[high-collision] MERGE benchmark is done, sampling from : " + state.mergeKeys.length+ " possible keys to merge.");
    }

    private static void evaluate(BenchmarkState state, Integer[] possibleKeys, BiConsumer<RocksTransaction, Integer> operator) {
        List<Future<?>> jobs = new ArrayList<>();
        for (int j = 0; j < 10_000; j++) {
            jobs.add(state.threadPool.submit(() -> {
                RocksTransaction tx = new RocksTransaction(state.db);
                for (int i = 0; i < 100; i++) {
                    int index = state.random.nextInt(possibleKeys.length);
                    int key = possibleKeys[index];
                    try {
                        // add noise key per key we care about
                        tx.putUntracked(toBytes(state.noiseKeys[state.random.nextInt(state.noiseKeys.length)]), toBytes(1));
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                    operator.accept(tx, key);
                }

                try {
                    tx.commit();
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }));
        }
        jobs.forEach(job -> {
            try {
                job.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

}
