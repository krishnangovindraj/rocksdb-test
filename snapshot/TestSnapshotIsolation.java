package rocksdbtest.snapshot;

import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestSnapshotIsolation {

    final static byte[] key = "ghi".getBytes(UTF_8);
    final static byte[] differentKey = "bye".getBytes(UTF_8);
    final static byte[] value = new byte[0];
//    final static byte[] value = differentKey;

    public static void doTest(OptimisticTransactionDB db) throws RocksDBException {
        testConcurrentPutThrows(db);
        testConcurrentDeleteThrows(db);
        testConcurrentPutUntrackedDoesNotThrow(db);
        testConcurrentPutUntrackedDeleteTrackedThrows(db);
        testConcurrentGetForUpdate(db);
    }

    private static void testConcurrentPutThrows(OptimisticTransactionDB db) throws RocksDBException {
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

    private static void testConcurrentDeleteThrows(OptimisticTransactionDB db) throws RocksDBException {
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

    private static void testConcurrentPutUntrackedDoesNotThrow(OptimisticTransactionDB db) throws RocksDBException {
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

    private static void testConcurrentPutUntrackedDeleteTrackedThrows(OptimisticTransactionDB db) throws RocksDBException {
        System.out.println("\n#### Testing that putUntracked clashing with delete, prexisting key, throws");
        final WriteOptions writeOptions1 = new WriteOptions();
        final OptimisticTransactionOptions txOptions = new OptimisticTransactionOptions().setSetSnapshot(true);

        Transaction tx1 = db.beginTransaction(writeOptions1, txOptions);

        System.out.println(tx1 + ", snapshot version: " + tx1.getSnapshot().getSequenceNumber());

        tx1.put(key, value);
        tx1.commit();

        Transaction tx2 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx3 = db.beginTransaction(writeOptions1, txOptions);

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
        Transaction tx4 = db.beginTransaction(writeOptions1, txOptions);
        Transaction tx5 = db.beginTransaction(writeOptions1, txOptions);

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

    // just testing:

    private static void testConcurrentGetForUpdate(OptimisticTransactionDB db) throws RocksDBException {
        System.out.println("\n#### Testing that putUntracked clashing with delete, prexisting key, throws");
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
        System.out.println("FAIL");
    }

    public static void main(String[] args) {
        String tempDir = System.getProperty("java.io.tmpdir");

        String dbPath = tempDir + "rocks_duplicate_key_2";

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
}
