package rocksdbtest.snapshot;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class TestSnapshotIsolation {

    final static byte[] key = "ghi".getBytes(UTF_8);
    final static byte[] differentKey = "bye".getBytes(UTF_8);
    final static byte[] value = new byte[0];

    private static Options options;
    private static OptimisticTransactionDB db;

    @BeforeClass
    public static void setup() throws RocksDBException {
        String tempDir = System.getProperty("java.io.tmpdir");
        String dbPath = tempDir + "rocks_duplicate_key_2";
        System.out.println("DB path is: " + dbPath);
        options = new Options();
        options.setCreateIfMissing(true);
        db = OptimisticTransactionDB.open(options, dbPath);
    }

    @AfterClass
    public static void tearDown() {
        options.close();
        db.close();
    }

    @Test
    public void testConcurrentGetAndGetExclusive() throws RocksDBException {
        final ReadOptions readOptions = new ReadOptions();
        final WriteOptions writeOptions1 = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx0 = db.beginTransaction(writeOptions1, txOptions);
        tx0.put(key, value);
        tx0.commit();

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx2 = db.beginTransaction(writeOptions1, txOptions);

        boolean existsTx1 = tx1.get(readOptions, key) != null;
        boolean existsTx2 = tx2.getForUpdate(readOptions, key, true) != null;
        if (existsTx2) tx2.delete(key);
        tx1.commit();
        tx2.commit();
    }

    @Test
    public void testConcurrentPutThrows() throws RocksDBException {
        System.out.println("\n#### Testing that concurrent PUTs throw");
        final WriteOptions writeOptions1 = new WriteOptions();
        final WriteOptions writeOptions2 = new WriteOptions();
        final ReadOptions readOptions = new ReadOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        final OptimisticTransactionOptions txOptions2 = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx2 = db.beginTransaction(writeOptions2, txOptions2);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());
        System.out.println(tx2 + ", snapshot version: " + tx2.getSnapshot().getSequenceNumber());


        tx1.put(key, value);
        tx2.put(key, value);
        tx1.commit();
        System.out.println("After commit tx 1, seq number is: " + db.getLatestSequenceNumber());

        // expect this to throw
        try {
            tx2.commit();
        } catch (Exception e) {
            System.out.println("==> second tx commit throws - SUCCESS");
            return;
        }
        System.out.println("FAIL");
    }

    @Test
    public void testConcurrentDeleteThrows() throws RocksDBException {
        System.out.println("\n#### Testing that concurrent DELETEs throw");
        final WriteOptions writeOptions1 = new WriteOptions();
        final WriteOptions writeOptions2 = new WriteOptions();
        final ReadOptions readOptions = new ReadOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        final OptimisticTransactionOptions txOptions2 = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());

        tx1.put(key, value);
        tx1.commit();
        System.out.println("After commit tx 1, seq number is: " + db.getLatestSequenceNumber());

        Transaction tx4 = db.beginTransaction(writeOptions2, txOptions2);
        Transaction tx3 = db.beginTransaction(writeOptions2, txOptions2);
        System.out.println(Arrays.toString(tx4.get(readOptions, key)));
        tx3.delete(key);
        tx4.delete(key);
        tx3.commit();
        System.out.println("After commit delete tx 3, seq number is: " + db.getLatestSequenceNumber());

        // expect this to throw
        try {
            tx4.commit();
        } catch (Exception e) {
            System.out.println("==> second commit, concurrent delete throws - SUCCESS");
            return;
        }
        System.out.println("==> SUCCESS");
    }

    @Test
    public void testConcurrentPutUntrackedDoesNotThrow() throws RocksDBException {
        System.out.println("\n#### Testing that concurrent PutUntracked don't throw");
        final WriteOptions writeOptions1 = new WriteOptions();
        final WriteOptions writeOptions2 = new WriteOptions();
        final ReadOptions readOptions = new ReadOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        final OptimisticTransactionOptions txOptions2 = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx2 = db.beginTransaction(writeOptions2, txOptions2);
        Transaction tx3 = db.beginTransaction(writeOptions2, txOptions2);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());
        System.out.println(tx2 + ", snapshot version: " + tx2.getSnapshot().getSequenceNumber());

        tx1.putUntracked(key, value);
        tx2.putUntracked(key, value);

        // check that the untracked respect the snapshot isolation, and not visible outside the tx until commit
        byte[] read = tx3.get(readOptions, key);
        System.out.println("Visible outside of the tx (we need this to be false): " + (read == null));
        tx1.commit();
        System.out.println("After commit tx 1, seq number is: " + db.getLatestSequenceNumber());
        tx2.commit();
        System.out.println("After commit tx 2, seq number is: " + db.getLatestSequenceNumber());
        System.out.println("==> SUCCESS");
    }

    @Test
    public void testConcurrentPutUntrackedDeleteTrackedThrows() throws RocksDBException {
        System.out.println("\n#### Testing that putUntracked clashing with delete, prexisting key, throws");
        final ReadOptions readOptions = new ReadOptions();
        final WriteOptions writeOptions = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions, txOptions);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());

        tx1.put(key, value);
        tx1.commit();

        Transaction tx2 = db.beginTransaction(writeOptions, txOptions);
        Transaction tx3 = db.beginTransaction(writeOptions, txOptions);

        tx2.putUntracked(key, value);
        tx3.delete(key);
        tx2.commit();
        try {
            // delete goes in last
            tx3.commit();
        } catch (Exception e) {
            System.out.println("==> concurrent delete after putUntracked commit second throws -- SUCCESS");
        }

        // try other order
        Transaction tx4 = db.beginTransaction(writeOptions, txOptions);
        Transaction tx5 = db.beginTransaction(writeOptions, txOptions);

        assertArrayEquals(value, tx4.get(readOptions, key));
        tx4.putUntracked(key, value);
        tx5.delete(key);
        tx5.commit();
        try {
            tx4.commit();
        } catch (Exception e) {
            System.out.println("FAIL -- not expected to throw");
            return;
        }
        System.out.println("==> SUCCESSS, does not throw");

    }

    @Test
    public void testConcurrentGetForUpdate() throws RocksDBException {
        System.out.println("\n#### Testing interaction between concurrent getForUpdate calls");
        final ReadOptions readOptions = new ReadOptions();
        final WriteOptions writeOptions1 = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());

        tx1.put(key, value);
        tx1.commit();

        Transaction tx2 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx3 = db.beginTransaction(writeOptions1, txOptions);

        // try non-exclusive GetForUpdate
        tx2.getForUpdate(readOptions, key, false);
        tx3.getForUpdate(readOptions, key, false);
        tx2.commit();
        tx3.commit();
        System.out.println("==> non-exclusive getForUpdate does not throw -- SUCCESS");

        Transaction tx4 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx5 = db.beginTransaction(writeOptions1, txOptions);
        tx4.getForUpdate(readOptions, key, true);
        tx5.getForUpdate(readOptions, key, true);
        tx4.commit();
        try {
            tx5.commit();
        } catch (Exception e) {
            System.out.println("==> exclusive getForUpdate THROWS, SUCCESS");
            return;
        }
        System.out.println("SUCCESS");
    }

    @Test
    public void testConcurrentPutsProtectedByGetUntracked() throws RocksDBException {
        System.out.println("\n#### Testing that getForUpdate -> null followed by putUntracked does not clash with concurrent getForUpdate -> null and putUntracked");
        final ReadOptions readOptions = new ReadOptions();
        final WriteOptions writeOptions1 = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx2 = db.beginTransaction(writeOptions1, txOptions);

        boolean existsTx1 = tx1.getForUpdate(readOptions, key, false) != null;
        boolean existsTx2 = tx2.getForUpdate(readOptions, key, false) != null;
        if (!existsTx1) tx1.putUntracked(key, value);
        if (!existsTx2) tx2.putUntracked(key, value);
        tx1.commit();
        tx2.commit();
    }


    @Test
    public void testConcurrentGetForUpdateAndPutUntrackedThrows() throws RocksDBException {
        System.out.println("\n#### Testing that putUntracked does not clash with concurrent getForUpdate");
        final ReadOptions readOptions = new ReadOptions();
        final WriteOptions writeOptions1 = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());

        tx1.put(key, value);
        tx1.commit();

        Transaction tx2 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx3 = db.beginTransaction(writeOptions1, txOptions);

        tx2.getForUpdate(readOptions, key, false);
        tx3.putUntracked(key, value);
        tx2.commit();
        tx3.commit();

        Transaction tx4 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx5 = db.beginTransaction(writeOptions1, txOptions);

        tx4.getForUpdate(readOptions, key, false);
        tx5.putUntracked(key, value);
        tx5.commit();
        tx4.commit();
    }

    @Test
    public void testConcurrentGetForUpdateAndDeleteThrows() throws RocksDBException {
        System.out.println("\n#### Testing that putUntracked clashing with delete, prexisting key, throws");
        final ReadOptions readOptions = new ReadOptions();
        final WriteOptions writeOptions = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        Transaction tx1 = db.beginTransaction(writeOptions, txOptions);

        tx1.put(key, value);
        tx1.commit();

        Transaction tx2 = db.beginTransaction(writeOptions, txOptions);
        Transaction tx3 = db.beginTransaction(writeOptions, txOptions);

        tx2.getForUpdate(readOptions, key, false);
        tx3.delete(key);
        tx2.commit();
        try {
            tx3.commit();
            fail(); // this would throw if we could make a write conflict with an earlier reserved read
        } catch (Exception e) {
            System.out.println("==> delete that is preceded by getForUpdate THROWS, SUCCESS");
        }

        Transaction tx4 = db.beginTransaction(writeOptions, txOptions);
        Transaction tx5 = db.beginTransaction(writeOptions, txOptions);

        tx4.getForUpdate(readOptions, key, false);
        tx5.delete(key);
        tx5.commit();
        try {
            tx4.commit();
            fail(); // this should also throw
        } catch (Exception ignored) {
            System.out.println("===> it turns out doing a getForUpdate after delete also THROWS");
        }

        Transaction tx6 = db.beginTransaction(writeOptions, txOptions);
        assertArrayEquals(null, tx6.get(readOptions, key));

        System.out.println("==> and we can see that the key is deleted");

        System.out.println("SUCCESS");
    }

}
