package rocksdbtest.microbenchmarks;

import org.openjdk.jmh.annotations.Benchmark;

import java.nio.ByteBuffer;

public class ByteBufferBenchmarks {

    @Benchmark
    public void buffer() {
        int data = 1234567;
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(data);
        byte[] arr = byteBuffer.array();
    }

    @Benchmark
    public void toBytes() {
        int data = 1234567;
        byte[] arr = new byte[] {
                (byte)(data >> 24),
                (byte)(data >> 16),
                (byte)(data >> 8),
                (byte) data,
        };
    }

//    private int toInt(byte[] data) {
//        if (data == null || data.length != 4) return 0x0;
//        // ----------
//        return  data[0] << 24  |
//                data[1] << 16  |
//                data[2] << 8   |
//                data[3];
//    }
}
