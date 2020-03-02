package grakn.rocksdbtest;

import grakn.rocksdbtest.graphdb.EdgeLabel;
import grakn.rocksdbtest.graphdb.GraphDB;
import grakn.rocksdbtest.graphdb.traversal.TraversalIterator;
import org.rocksdb.*;

import java.util.Iterator;

public class TestDB {

    public static void main(String[] args) {
        String tempDir = System.getProperty("java.io.tmpdir");

        System.out.println("TempDir is: " + tempDir);

        String dbPath = tempDir + "db";

        try (final Options options = new Options();
             final Filter bloomFilter = new BloomFilter(10);
             final ReadOptions readOptions = new ReadOptions();) {
//             final Statistics stats = new Statistics();

            options.setCreateIfMissing(true);
//                    .setStatistics(stats)
//                    .setWriteBufferSize(8 * SizeUnit.KB)
//                    .setMaxWriteBufferNumber(3)
//                    .setMaxBackgroundCompactions(10)
//                    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
//                    .setCompactionStyle(CompactionStyle.LEVEL);

            options.setMemTableConfig(
                    new HashSkipListMemTableConfig());

            options.setAllowConcurrentMemtableWrite(false);

            options.useFixedLengthPrefixExtractor(9);

            final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
            table_options.setFilterPolicy(bloomFilter)
//                    .setBlockSizeDeviation(5)
//                    .setBlockRestartInterval(10)
                    .setCacheIndexAndFilterBlocks(true);

            options.setTableFormatConfig(table_options);

            //options.allowConcurrentMemtableWrite();

            try (final RocksDB db = RocksDB.open(options, dbPath)) {

                try (final GraphDB graphDB = new GraphDB(db)) {
                    graphDB.insertEdge(1, EdgeLabel.HAS.out(), 2);
                    graphDB.insertEdge(1, EdgeLabel.HAS.out(), 3);
                    graphDB.insertEdge(1, EdgeLabel.HAS.out(), 4);
                    graphDB.insertEdge(1, EdgeLabel.HASA.out(), 2, 5);
                    graphDB.insertEdge(1, EdgeLabel.HASA.out(), 2, 6);
                    graphDB.insertEdge(1, EdgeLabel.HASA.out(), 3, 6);
                    graphDB.insertEdge(1, EdgeLabel.HASA.out(), 2, 7);

                    try (TraversalIterator iterator = graphDB.edges(1, EdgeLabel.HAS.out())) {
                        if (iterator.seek()) do {
                            System.out.println(iterator.get());
                        } while (iterator.next());
                    }
                    try (TraversalIterator iterator = graphDB.edges(2, EdgeLabel.HAS.out())) {
                        if (iterator.seek()) do {
                            System.out.println(iterator.get());
                        } while (iterator.next());
                    }
                    try (TraversalIterator iterator = graphDB.edges(3, EdgeLabel.HAS.in())) {
                        if (iterator.seek()) do {
                            System.out.println(iterator.get());
                        } while (iterator.next());
                    }

                    System.out.println("1 hasa out via 2:");
                    try (TraversalIterator iterator = graphDB.edges(1, EdgeLabel.HASA.out(), 2)) {
                        if (iterator.seek()) do {
                            System.out.println(iterator.get());
                        } while (iterator.next());
                    }

                    try (RocksIterator iter = db.newIterator()) {
                        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                            System.out.println(bytesToHex(iter.key()));
                        }
                    }
                }
            } catch (RocksDBException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
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