package rocksdbtest.transaction;

import org.rocksdb.Checkpoint;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

public class RocksDatabase implements AutoCloseable {

    private final Options options;
    private final OptimisticTransactionDB db;

    public RocksDatabase(String dbPath) throws RocksDBException {
        options = new Options().setCreateIfMissing(true);
        db = OptimisticTransactionDB.open(options, dbPath);
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    public Transaction beginTransaction(WriteOptions writeOptions, OptimisticTransactionOptions optimisticTransactionOptions) {
        return db.beginTransaction(writeOptions, optimisticTransactionOptions);
    }

    public Snapshot getSnapshot() {
        return db.getSnapshot();
    }

    public Checkpoint createCheckpoint() {
        return Checkpoint.create(db);
    }
}
