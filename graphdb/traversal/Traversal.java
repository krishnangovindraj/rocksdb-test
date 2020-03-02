package grakn.rocksdbtest.graphdb.traversal;

import java.util.List;

public interface Traversal {

    Traversal has(long node); // Filter by those that have a node of a particular type

    Traversal isa(long node); // Transitive filter

    Traversal attr(long type, String attr);

    List<Long> get();
}
