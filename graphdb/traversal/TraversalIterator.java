package grakn.rocksdbtest.graphdb.traversal;

public interface TraversalIterator extends AutoCloseable {

    boolean seek();

    boolean next();

    long get();

    @Override
    void close();
}
