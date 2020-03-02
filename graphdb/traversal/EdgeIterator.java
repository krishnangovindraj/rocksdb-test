package grakn.rocksdbtest.graphdb.traversal;

import grakn.rocksdbtest.graphdb.KeySerDes;
import org.rocksdb.RocksIterator;

public class EdgeIterator implements TraversalIterator {
    private final RocksIterator iter;
    private final byte[] target;

    public EdgeIterator(RocksIterator iter, long nodeId, byte label) {
        this.iter = iter;
        this.target = KeySerDes.serialize(nodeId, label);
    }

    private boolean valid() {
        return iter.isValid() && KeySerDes.hasOther(iter.key()) && KeySerDes.prefixed(iter.key(), target);
    }

    @Override
    public boolean seek() {
        iter.seek(target);
        return valid();
    }

    @Override
    public boolean scan(long otherId) {
        iter.seek(KeySerDes.serialize(KeySerDes.getNode(target), KeySerDes.getEdge(target), otherId));
        return valid();
    }

    @Override
    public boolean next() {
        iter.next();
        return valid();
    }

    @Override
    public long get() {
        return KeySerDes.getOther(iter.key());
    }

    @Override
    public void close() {
        iter.close();
    }
}
