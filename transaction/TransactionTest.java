package rocksdbtest.transaction;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static java.util.Comparator.reverseOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionTest {

    static String dbPath;

    static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    @BeforeClass
    public static void beforeClass() {
        dbPath = TEMP_DIR + "rocks_transaction_testdb";
        System.out.println("DB path is: " + dbPath);
    }

    @Before
    public void before() throws IOException {
        if (Files.exists(Paths.get(dbPath))) {
            Files.walk(Paths.get(dbPath)).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    @Test
    public void put_data_is_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));

                assertEquals(3, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_data_is_not_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_data_is_not_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();

                assertEquals(0, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_update_snapshot_data_is_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();

                tx.updateReadSnapshot(db);
                assertEquals(3, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_data_is_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(3, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_put_commit_no_data_is_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{0, 0}, getInts(tx, 1, 5));

                tx.put(toBytes(1), toBytes(2));
                tx.commit();

                tx.put(toBytes(5), toBytes(8));
                tx.commit();

                assertArrayEqualsVerbose(new int[]{0, 0}, getInts(tx, 1, 5));
            }
        }
    }

    @Test
    public void put_commit_put_commit_all_data_is_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                tx.put(toBytes(1), toBytes(2));
                tx.commit();

                tx.put(toBytes(5), toBytes(8));
                tx.commit();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{2, 8}, getInts(tx, 1, 5));
            }
        }
    }

    @Test
    public void put_rollback_data_is_not_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.rollback();

                assertEquals(0, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_rollback_data_is_not_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.rollback();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_rollback_commit_data_is_not_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.rollback();
                tx.commit();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_rollback_data_is_not_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();
                tx.rollback();

                assertEquals(0, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_rollback_data_is_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(0, getInt(tx, 2));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();
                tx.rollback();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertEquals(3, getInt(tx, 2));
            }
        }
    }

    @Test
    public void put_commit_put_rollback_put_commit_no_data_is_readable() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{0, 0, 0}, getInts(tx, 2, 5, 13));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();

                tx.put(toBytes(5), toBytes(8));
                tx.rollback();

                tx.put(toBytes(13), toBytes(21));
                tx.commit();

                assertArrayEqualsVerbose(new int[]{0, 0, 0}, getInts(tx, 2, 5, 13));
            }
        }
    }

    @Test
    public void put_commit_put_rollback_put_commit_only_committed_data_is_persisted() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{0, 0, 0}, getInts(tx, 2, 5, 13));

                tx.put(toBytes(2), toBytes(3));
                tx.commit();

                tx.put(toBytes(5), toBytes(8));
                tx.rollback();

                tx.put(toBytes(13), toBytes(21));
                tx.commit();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{3, 0, 21}, getInts(tx, 2, 5, 13));
            }
        }
    }

    @Test
    public void uncommitted_data_is_not_visible_to_other_transaction_opened_before_put() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            RocksTransaction tx1 = null;
            RocksTransaction tx2 = null;
            try {
                tx1 = new RocksTransaction(db);
                assertEquals(0, getInt(tx1, 2));

                tx2 = new RocksTransaction(db);

                tx1.put(toBytes(2), toBytes(3));

                assertEquals(0, getInt(tx2, 2));
            } finally {
                if (tx1 != null) tx1.close();
                if (tx2 != null) tx2.close();
            }
        }
    }

    @Test
    public void uncommitted_data_is_not_visible_to_other_transaction_opened_after_put() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            RocksTransaction tx1 = null;
            RocksTransaction tx2 = null;
            try {
                tx1 = new RocksTransaction(db);
                assertEquals(0, getInt(tx1, 2));

                tx1.put(toBytes(2), toBytes(3));

                tx2 = new RocksTransaction(db);
                assertEquals(0, getInt(tx2, 2));
            } finally {
                if (tx1 != null) tx1.close();
                if (tx2 != null) tx2.close();
            }
        }
    }

    @Test
    public void committed_data_is_not_visible_to_other_transaction_opened_before_commit() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            RocksTransaction tx1 = null;
            RocksTransaction tx2 = null;
            try {
                tx1 = new RocksTransaction(db);
                assertEquals(0, getInt(tx1, 2));

                tx1.put(toBytes(2), toBytes(3));

                tx2 = new RocksTransaction(db);

                tx1.commit();

                assertEquals(0, getInt(tx2, 2));
            } finally {
                if (tx1 != null) tx1.close();
                if (tx2 != null) tx2.close();
            }
        }
    }

    @Test
    public void committed_data_is_visible_to_other_transaction_opened_after_commit() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            RocksTransaction tx1 = null;
            RocksTransaction tx2 = null;
            try {
                tx1 = new RocksTransaction(db);
                assertEquals(0, getInt(tx1, 2));

                tx1.put(toBytes(2), toBytes(3));
                tx1.commit();

                tx2 = new RocksTransaction(db);
                assertEquals(3, getInt(tx2, 2));
            } finally {
                if (tx1 != null) tx1.close();
                if (tx2 != null) tx2.close();
            }
        }
    }

    @Test
    public void when_concurrent_transactions_put_and_commit_same_key_second_commit_throws() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{0, 0}, new int[]{getInt(tx1, 2), getInt(tx2, 2)});

                tx1.put(toBytes(2), toBytes(3));
                tx2.put(toBytes(2), toBytes(4));
                // race to commit ...

                tx1.commit();
                // tx2 loses the race and its commit is rejected
                assertThrowsRocksDBException(tx2::commit);
            }
        }
    }

    @Test
    public void concurrent_transactions_can_put_and_commit_different_keys() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{0, 0}, new int[]{getInt(tx1, 2), getInt(tx2, 5)});

                tx1.put(toBytes(2), toBytes(3));
                tx2.put(toBytes(5), toBytes(8));

                tx1.commit();
                tx2.commit();
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{3, 8}, getInts(tx, 2, 5));
            }
        }
    }

    @Test
    public void concurrent_transactions_commits_are_invisible_to_each_other() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{0, 0, 0, 0}, new int[]{
                        getInt(tx1, 2), getInt(tx1, 5),
                        getInt(tx2, 2), getInt(tx2, 5)});

                tx1.put(toBytes(2), toBytes(3));
                tx2.put(toBytes(5), toBytes(8));

                tx1.commit();
                tx2.commit();

                assertArrayEqualsVerbose(new int[]{0, 0}, new int[]{
                        getInt(tx1, 5),
                        getInt(tx2, 2)
                });
            }
        }
    }

    @Test
    public void a_transaction_commits_even_if_it_checked_a_condition_that_a_concurrent_transaction_violated() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{0, 0}, getInts(tx, 2, 3));

                tx.put(toBytes(1), toBytes(10));
                tx.commit();
            }
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{10, 10}, new int[]{getInt(tx1, 1), getInt(tx2, 1)});

                final int balance1 = getInt(tx1, 1);
                if (balance1 >= 7) {
                    tx1.put(toBytes(2), toBytes(12));
                    tx1.commit();
                }

                final int balance2 = getInt(tx2, 1);
                if (balance2 >= 8) {
                    tx2.put(toBytes(3), toBytes(16));
                    tx2.commit();
                }
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{12, 16}, getInts(tx, 2, 3));
            }
        }
    }

    @Test
    public void concurrent_update_and_rollback_maintain_isolation() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            RocksTransaction tx1 = null;
            RocksTransaction tx2 = null;
            RocksTransaction tx3 = null;
            try {
                tx1 = new RocksTransaction(db);
                tx2 = new RocksTransaction(db);
                assertEquals(0, getInt(tx1, 2));

                tx1.put(toBytes(2), toBytes(20));
                tx2.put(toBytes(2), toBytes(10));

                tx2.commit();
                tx1.rollback();

                assertEquals(0, getInt(tx1, 2));

                tx3 = new RocksTransaction(db);
                tx3.put(toBytes(2), toBytes(30));
                tx3.commit();

                assertEquals(0, getInt(tx1, 2));
            } finally {
                if (tx1 != null) tx1.close();
                if (tx2 != null) tx2.close();
                if (tx3 != null) tx3.close();
            }
        }
    }

    @Test
    public void non_exclusive_lock_causes_transaction_to_fail_if_other_transaction_modifies_the_lock() throws RocksDBException {
        try (RocksDatabase db = new RocksDatabase(dbPath)) {
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{0, 0}, getInts(tx, 2, 3));

                tx.put(toBytes(1), toBytes(10));
                tx.commit();
            }
            try (RocksTransaction tx1 = new RocksTransaction(db);
                 RocksTransaction tx2 = new RocksTransaction(db)) {

                assertArrayEqualsVerbose(new int[]{10, 10}, new int[]{getInt(tx1, 1), getInt(tx2, 1)});

                final int balance1 = toInt(tx1.getForUpdate(toBytes(1), false));
                if (balance1 >= 7) {
                    tx1.put(toBytes(2), toBytes(12));
                    // slow transaction ...
                }

                final int balance2 = getInt(tx2, 1);
                if (balance2 >= 8) {
                    tx2.put(toBytes(3), toBytes(16));
                    tx2.put(toBytes(1), toBytes(balance2 - 8));
                    tx2.commit();
                }

                // transaction 1 fails because key:1 was modified
                assertThrowsRocksDBException(tx1::commit);
            }
            try (RocksTransaction tx = new RocksTransaction(db)) {
                assertArrayEqualsVerbose(new int[]{2, 0, 16}, getInts(tx, 1, 2, 3));
            }
        }
    }

    /* Behaviour of an exclusive lock is documented in TestSnapshotIsolation */

    private static void assertArrayEqualsVerbose(int[] expected, int[] actual) {
        assertArrayEquals("Expected " + Arrays.toString(expected) + " but was " + Arrays.toString(actual), expected, actual);
    }

    @FunctionalInterface
    interface RocksCode {
        void run() throws RocksDBException;
    }

    private static void assertThrowsRocksDBException(RocksCode rocksCode) {
        boolean threw = false;
        try {
            rocksCode.run();
        } catch (RocksDBException e) {
            threw = true;
        }
        assertTrue(threw);
    }

    private static byte[] toBytes(final int data) {
        return new byte[] {
                (byte)(data >> 24),
                (byte)(data >> 16),
                (byte)(data >> 8),
                (byte) data,
        };
    }

    private static int toInt(byte[] data) {
        if (data == null || data.length != 4) return 0;
        // ----------
        return  data[0] << 24  |
                data[1] << 16  |
                data[2] << 8   |
                data[3];
    }

    private static int getInt(RocksTransaction tx, int key) throws RocksDBException {
        return toInt(tx.get(toBytes(key)));
    }

    private static int[] getInts(RocksTransaction tx, int... keys) throws RocksDBException {
        final int[] ints = new int[keys.length];
        for (int i = 0; i < keys.length; i++) ints[i] = getInt(tx, keys[i]);
        return ints;
    }
}
