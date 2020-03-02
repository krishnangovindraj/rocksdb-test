package grakn.rocksdbtest.graphdb.traversal;

import grakn.rocksdbtest.graphdb.KeySerDes;
import org.rocksdb.RocksIterator;

public class TernaryEdgeIterator extends EdgeIterator {
    public TernaryEdgeIterator(RocksIterator iter, long start, byte label, long via) {
        super(iter, KeySerDes.serialize(start, label, via));
    }

    @Override
    protected long get(byte[] key) {
        return KeySerDes.getOther(key);
    }

    @Override
    protected boolean check(byte[] key) {
        return KeySerDes.hasOther(key);
    }
}
