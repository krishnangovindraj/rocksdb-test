package rocksdbtest.transaction;

import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

class RocksTransaction implements AutoCloseable {

    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final OptimisticTransactionOptions optimisticTransactionOptions;
    private final Transaction transaction;

    RocksTransaction(RocksDatabase db) {
        writeOptions = new WriteOptions();
        readOptions = new ReadOptions();
        optimisticTransactionOptions = new OptimisticTransactionOptions().setSetSnapshot(true);
        transaction = db.beginTransaction(writeOptions, optimisticTransactionOptions);
        readOptions.setSnapshot(transaction.getSnapshot());
    }

    @Override
    public void close() {
        transaction.close();
        optimisticTransactionOptions.close();
        readOptions.close();
        writeOptions.close();
    }

    void put(byte[] key, byte[] value) throws RocksDBException {
        transaction.put(key, value);
    }

    byte[] get(byte[] key) throws RocksDBException {
        return transaction.get(readOptions, key);
    }

    byte[] getForUpdate(byte[] key, boolean exclusive) throws RocksDBException {
        return transaction.getForUpdate(readOptions, key, exclusive);
    }

    void updateReadSnapshot(RocksDatabase db) {
        readOptions.setSnapshot(db.getSnapshot());
    }

    void commit() throws RocksDBException {
        transaction.commit();
    }

    void rollback() throws RocksDBException {
        transaction.rollback();
    }
}
