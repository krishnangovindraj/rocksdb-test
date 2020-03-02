package grakn.rocksdbtest.graphdb;

import grakn.rocksdbtest.graphdb.traversal.BinaryEdgeIterator;
import grakn.rocksdbtest.graphdb.traversal.TernaryEdgeIterator;
import grakn.rocksdbtest.graphdb.traversal.TraversalIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.util.Iterator;

public class GraphDB implements AutoCloseable {
    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;

    private static final byte[] EMPTY = {1};

    public GraphDB(RocksDB db) {
        this.db = db;
        this.writeOptions = new WriteOptions();
        this.readOptions = new ReadOptions();
    }

    public TraversalIterator edges(long start, byte label) {
        return new BinaryEdgeIterator(db.newIterator(), start, label);
    }

    public TraversalIterator edges(long start, byte label, long via) {
        return new TernaryEdgeIterator(db.newIterator(), start, label, via);
    }

    public void insertEdge(long start, byte label, long via) throws RocksDBException {
        System.out.println(KeySerDes.serialize(start, label, via).length);
        System.out.println(bytesToHex(KeySerDes.serialize(start, label, via)));

        db.put(KeySerDes.serialize(start, label, via), EMPTY);
        db.put(KeySerDes.serialize(start, (byte) (label ^ 0x80), via), EMPTY);

        System.out.println(bytesToHex(db.get(KeySerDes.serialize(start, label, via))));
    }

    public void insertEdge(long start, byte label, long via, long other) throws RocksDBException {
        db.put(KeySerDes.serialize(start, label, via, other), EMPTY);
    }

    @Override
    public void close() {
        readOptions.close();
        writeOptions.close();
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
