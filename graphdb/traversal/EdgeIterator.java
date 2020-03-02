package grakn.rocksdbtest.graphdb.traversal;

import grakn.rocksdbtest.graphdb.KeySerDes;
import org.rocksdb.RocksIterator;

public abstract class EdgeIterator implements TraversalIterator {
    private final RocksIterator iter;
    private final byte[] target;

    public EdgeIterator(RocksIterator iter, byte[] target) {
        this.iter = iter;
        this.target = target;
    }

    private boolean valid() {
        return iter.isValid() && KeySerDes.prefixed(iter.key(), target) && check(iter.key());
    }

    @Override
    public boolean seek() {
        iter.seek(target);
        return valid();
    }

    @Override
    public boolean next() {
        iter.next();
        return valid();
    }

    @Override
    public long get() {
        return get(iter.key());
    }

    protected abstract long get(byte[] key);

    protected abstract boolean check(byte[] key);

    @Override
    public void close() {
        iter.close();
    }
}
