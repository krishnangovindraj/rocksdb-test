package rocksdbtest.spliteratortest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import rocksdbtest.base.KeyTestDB;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RocksKeySpliteratorTest {

    private static final int NODE_NUMBER = 100;
    private static final long SLEEP_TIME = 1;

    private static KeyTestDB db;

    @BeforeClass
    public static void dbSetup() throws RocksDBException {
        db = new KeyTestDB();
        db.addRandomNodes(2, NODE_NUMBER, 1, 10);
    }

    @AfterClass
    public static void dbTeardown() {
        db.close();
    }

    private static IntStream stream(int key) {
        return stream(key, false);
    }

    private static IntStream parallelStream(int key) {
        return stream(key, true);
    }

    private static IntStream stream(int key, boolean parallel) {
        RocksKeySpliterator spliterator = new RocksKeySpliterator(() -> db.iterateKey(key));
        return StreamSupport.stream(spliterator, parallel).mapToInt(KeyTestDB::getEnd).peek(k -> sleep());
    }

    @Test
    public void spliterateAll() {
        System.out.println("Spliterate all");
        for (int iteration = 0; iteration < 5; iteration++) {
            long timeSpentInSequential = 0;
            long timeSpentInParallel = 0;

            for (int i = 0; i < NODE_NUMBER; ++i) {
                Set<Integer> control = new HashSet<>();
                try (RocksIterator iterator = db.iterateKey(i)) {
                    for (; iterator.isValid(); iterator.next()) {
                        control.add(KeyTestDB.getEnd(iterator.key()));
                    }
                }

                timeSpentInSequential += testSpliterate(control, false, i);
                timeSpentInParallel += testSpliterate(control, true, i);
            }

            System.out.println("Iteration " + iteration);
            System.out.println("Sequential: " + nsToString(timeSpentInSequential));
            System.out.println("Parallel  : " + nsToString(timeSpentInParallel));
        }
    }

    @Test
    public void recursion() {
        System.out.println("Recursion traversal");
        for (int iteration = 0; iteration < 5; iteration++) {
            long timeSpentInSequential = 0;
            long timeSpentInParallel = 0;

            for (int i = 0; i < NODE_NUMBER; ++i) {

                final int j = i;
                timeSpentInSequential += time(() -> {
                    int[] array = stream(j)
                            .flatMap(RocksKeySpliteratorTest::stream)
                            .flatMap(RocksKeySpliteratorTest::stream)
                            .toArray();
                });
                timeSpentInParallel += time(() -> {
                    int[] array = parallelStream(j)
                            .flatMap(RocksKeySpliteratorTest::parallelStream)
                            .flatMap(RocksKeySpliteratorTest::parallelStream)
                            .toArray();
                });
            }

            System.out.println("Iteration " + iteration);
            System.out.println("Sequential: " + nsToString(timeSpentInSequential));
            System.out.println("Parallel  : " + nsToString(timeSpentInParallel));
        }
    }

    private static long time(Runnable runnable) {
        long startTime = System.nanoTime();
        runnable.run();
        return System.nanoTime() - startTime;
    }

    private static void sleep() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private long testSpliterate(Set<Integer> control, boolean parallel, int key) {
        List<Integer> listResult;

        final long startTime = System.nanoTime();
        RocksKeySpliterator spliterator = new RocksKeySpliterator(() -> db.iterateKey(key));

        Stream<byte[]> keyStream = StreamSupport.stream(spliterator, parallel);

        listResult = keyStream.peek(k -> sleep()).map(KeyTestDB::getEnd).collect(Collectors.toList());
        final long endTime = System.nanoTime();
        final long timeSpent = endTime - startTime;

        Set<Integer> result = new HashSet<>();
        listResult.forEach(e -> {
            if (result.contains(e)) fail();
            result.add(e);
        });

        assertEquals(control, result);
        return timeSpent;
    }


    public static String nsToString(long ns) {
        long duration = ns;
        long nanoseconds = duration % 1000;
        duration /= 1000;
        long microseconds = duration % 1000;
        duration /= 1000;
        long milliseconds = duration % 1000;
        duration /= 1000;
        long seconds = duration;
        return String.format("%7d.%03dm%03du%03dns", seconds, milliseconds, microseconds, nanoseconds);
    }

    private static double nanosInSeconds(long nanos) {
        return ((double) nanos) / 1_000_000_000d;
    }
}
