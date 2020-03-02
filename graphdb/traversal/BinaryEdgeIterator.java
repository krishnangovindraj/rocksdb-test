package grakn.rocksdbtest.graphdb.traversal;

import grakn.rocksdbtest.graphdb.KeySerDes;
import org.rocksdb.RocksIterator;

public class BinaryEdgeIterator extends EdgeIterator {

    public BinaryEdgeIterator(RocksIterator iter, long start, byte label) {
        super(iter, KeySerDes.serialize(start, label));
    }

    @Override
    protected long get(byte[] key) {
        return KeySerDes.getVia(key);
    }

    @Override
    protected boolean check(byte[] key) {
        return KeySerDes.hasVia(key);
    }
}
