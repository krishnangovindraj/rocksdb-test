package grakn.rocksdbtest.graphdb;

import java.nio.ByteBuffer;

public class KeySerDes {

    private static final int NODE = Long.BYTES;
    private static final int NODE_EDGE = NODE + Byte.BYTES;
    private static final int NODE_EDGE_VIA = NODE_EDGE + Long.BYTES;
    private static final int NODE_EDGE_VIA_OTHER = NODE_EDGE_VIA + Long.BYTES;

    public static byte[] serialize(long nodeId) {
        return ByteBuffer.allocate(NODE)
                .putLong(nodeId).array();
    }

    public static byte[] serialize(long nodeId, byte edgeLabel) {
        return ByteBuffer.allocate(NODE_EDGE)
                .putLong(nodeId)
                .put(edgeLabel).array();
    }

    public static byte[] serialize(long nodeId, byte edgeLabel, long viaId) {
        return ByteBuffer.allocate(NODE_EDGE_VIA)
                .putLong(nodeId)
                .put(edgeLabel)
                .putLong(viaId).array();
    }

    public static byte[] serialize(long nodeId, byte edgeLabel, long viaId, long otherId) {
        return ByteBuffer.allocate(NODE_EDGE_VIA_OTHER)
                .putLong(nodeId)
                .put(edgeLabel)
                .putLong(viaId)
                .putLong(otherId).array();
    }

    public static long getNode(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static boolean hasEdge(byte[] bytes) {
        return bytes.length >= NODE_EDGE;
    }

    public static byte getEdge(byte[] bytes) {
        return bytes[NODE];
    }

    public static boolean hasVia(byte[] bytes) {
        return bytes.length >= NODE_EDGE_VIA;
    }

    public static long getVia(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.position(NODE_EDGE);
        return buffer.getLong();
    }

    public static boolean hasOther(byte[] bytes) {
        return bytes.length >= NODE_EDGE_VIA_OTHER;
    }

    public static long getOther(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.position(NODE_EDGE_VIA);
        return buffer.getLong();
    }

    public static boolean prefixed(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) return false;
        }
        return true;
    }
}
