package rocksdbtest.checkpoint;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDBException;
import rocksdbtest.transaction.RocksDatabase;
import rocksdbtest.transaction.RocksTransaction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.Comparator.reverseOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static rocksdbtest.transaction.RocksTransaction.getInt;
import static rocksdbtest.transaction.RocksTransaction.toBytes;

public class CheckpointTest {

    static String dbPath;

    static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    @BeforeClass
    public static void beforeClass() {
        dbPath = TEMP_DIR + "_rocks_checkpoint_test_db";
        System.out.println("DB path is: " + dbPath);
    }

    @Before
    public void before() throws IOException {
        if (Files.exists(Paths.get(dbPath))) {
            Files.walk(Paths.get(dbPath)).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    @Test
    public void create_checkpoint() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{-1, -1}, new int[]{getInt(tx1, 2), getInt(tx2, 5)});

                tx1.put(toBytes(2), toBytes(3));
                tx2.put(toBytes(5), toBytes(8));

                tx1.commit();
                tx2.commit();
            }

            Checkpoint checkpoint = db.createCheckpoint();
            checkpoint.createCheckpoint(TEMP_DIR + "_create_checkpoint");
        }
    }

    @Test
    public void restore_checkpoint() throws RocksDBException {
        String checkpointPath = TEMP_DIR + "_restore_checkpoint";
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{-1, -1}, new int[]{getInt(tx1, 2), getInt(tx2, 5)});

                tx1.put(toBytes(2), toBytes(3));
                tx2.put(toBytes(5), toBytes(8));

                tx1.commit();
                tx2.commit();
            }

            Checkpoint checkpoint = db.createCheckpoint();
            checkpoint.createCheckpoint(checkpointPath);

            try (RocksTransaction tx3 = new RocksTransaction(db)) {
                tx3.delete(toBytes(2));
                tx3.commit();
            }

            try (RocksTransaction tx4 = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{-1, 8}, new int[]{getInt(tx4, 2), getInt(tx4, 5)});
            }
        }

        try (RocksDatabase db = new RocksDatabase(checkpointPath)) {

            try (RocksTransaction tx1 = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{3, 8}, new int[]{getInt(tx1, 2), getInt(tx1, 5)});
            }
        }
    }

    @Test
    public void restore_copied_checkpoint() throws RocksDBException, IOException {
        String receivedCheckpointPath = TEMP_DIR + "_restore_checkpoint_received";
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{-1, -1}, new int[]{getInt(tx1, 2), getInt(tx2, 5)});

                tx1.put(toBytes(2), toBytes(3));
                tx2.put(toBytes(5), toBytes(8));

                tx1.commit();
                tx2.commit();
            }

            Checkpoint checkpoint = db.createCheckpoint();
            String checkpointPath = TEMP_DIR + "_restore_copied_checkpoint";
            checkpoint.createCheckpoint(checkpointPath);

            try (RocksTransaction tx3 = new RocksTransaction(db)) {
                tx3.delete(toBytes(2));
                tx3.commit();
            }

            try (RocksTransaction tx4 = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{-1, 8}, new int[]{getInt(tx4, 2), getInt(tx4, 5)});
            }

            copyDirectory(Paths.get(checkpointPath), Paths.get(receivedCheckpointPath));
        }

        try (RocksDatabase db = new RocksDatabase(receivedCheckpointPath)) {

            try (RocksTransaction tx1 = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{3, 8}, new int[]{getInt(tx1, 2), getInt(tx1, 5)});
            }
        }
    }

    @Test
    public void restore_large_checkpoint() throws RocksDBException {
        String checkpointPath = TEMP_DIR + "_restore_large_checkpoint";
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {

                for (int i = 0; i < 1_000_000; i++) {
                    tx.put(toBytes(i), toBytes(i*2));
                }

                tx.commit();
            }

            Checkpoint checkpoint = db.createCheckpoint();
            checkpoint.createCheckpoint(checkpointPath);

            try (RocksTransaction tx = new RocksTransaction(db)) {

                for (int i = 0; i < 500_000; i++) {
                    tx.delete(toBytes(i));
                }

                tx.commit();
            }

            try (RocksTransaction tx4 = new RocksTransaction(db)) {
                assertEquals(-1, getInt(tx4, 270_000));
            }
        }

        try (RocksDatabase db = new RocksDatabase(checkpointPath)) {

            try (RocksTransaction tx4 = new RocksTransaction(db)) {
                assertEquals(700_000, getInt(tx4, 350_000));
            }
        }
    }

    public void copyDirectory(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
        }
    }

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /* Behaviour of an exclusive lock is documented in TestSnapshotIsolation */

    private static void assertArrayEqualsVerbose(int[] expected, int[] actual) {
        assertArrayEquals("Expected " + Arrays.toString(expected) + " but was " + Arrays.toString(actual), expected, actual);
    }
}
