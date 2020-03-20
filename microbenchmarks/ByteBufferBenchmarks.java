package rocksdbtest.microbenchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;

import java.nio.ByteBuffer;

public class ByteBufferBenchmarks {

    static final int DATA = 1234567;

    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    public byte[] buffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(DATA);
        return byteBuffer.array();
    }

    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.Throughput)
    @Benchmark
    public byte[] toBytes() {
        return new byte[] {
                (byte)(DATA >> 24),
                (byte)(DATA >> 16),
                (byte)(DATA >> 8),
                (byte) DATA,
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
