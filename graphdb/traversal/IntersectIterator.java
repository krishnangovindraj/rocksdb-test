package grakn.rocksdbtest.graphdb.traversal;

public class IntersectIterator implements TraversalIterator {
    TraversalIterator first;
    TraversalIterator second;
    long firstRes;
    long secondRes;

    public IntersectIterator(TraversalIterator first, TraversalIterator second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean seek() {
        if (!first.seek() || !second.seek()) {
            return false;
        }
        firstRes = first.get();
        secondRes = second.get();
        scan();
        return true;
    }

    @Override
    public boolean scan(long otherId) {
        assert otherId > firstRes;
        assert otherId > secondRes;
        if (!first.scan(otherId)) {
            return false;
        }
        firstRes = first.get();
        if (!second.scan(firstRes)) {
            return false;
        }
        secondRes = second.get();
        return scan();
    }

    @Override
    public boolean next() {
        first.next();
        firstRes = first.get();
        if (!second.scan(firstRes)) {
            return false;
        }
        secondRes = second.get();
        return scan();
    }

    private boolean scan() {
        while (firstRes != secondRes) {
            if (firstRes < secondRes) {
                if (!first.scan(secondRes)) {
                    return false;
                }
                firstRes = first.get();
            } else {
                if (!second.scan(firstRes)) {
                    return false;
                }
                secondRes = first.get();
            }
        }
        return true;
    }

    @Override
    public long get() {
        return firstRes;
    }

    @Override
    public void close() {
        first.close();
        second.close();
    }
}
