package grakn.rocksdbtest.graphdb;

public enum EdgeLabel {
    HAS(0),
    HASA(1),
    SUB(2),
    ISA(3);

    private final byte key;

    EdgeLabel(int key) {
        this.key = (byte) key;
    }

    public byte out() {
        return key;
    }

    public byte in() {
        return (byte) (key ^ 0x80);
    }
}
