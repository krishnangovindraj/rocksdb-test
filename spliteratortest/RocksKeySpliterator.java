package rocksdbtest.spliteratortest;

import org.rocksdb.RocksIterator;

import java.util.ArrayList;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RocksKeySpliterator implements Spliterator<byte[]> {

    private BatchedSource source;
    private List<byte[]> buffer;
    private int pointer = 0;

    /**
     * @param iteratorSupplier A supplier for a bounded iterator that has be pre-seeked
     */
    public RocksKeySpliterator(Supplier<RocksIterator> iteratorSupplier) {
        this.source = new BatchedSource(iteratorSupplier);
    }

    private RocksKeySpliterator(BatchedSource source, List<byte[]> buffer) {
        this.source = source;
        this.buffer = buffer;
    }

    @Override
    public boolean tryAdvance(Consumer<? super byte[]> action) {
//        System.out.println("Thread: " + Thread.currentThread().getName() + " tryAdvance");
        if (buffer == null || pointer >= buffer.size()) {
            buffer = source.nextBatch();
        }
        if (buffer == null) {
            return false;
        }
        action.accept(buffer.get(pointer++));
        return true;
    }

    @Override
    public void forEachRemaining(Consumer<? super byte[]> action) {
//        System.out.println("Thread: " + Thread.currentThread().getName() + " forEachRemaining");
        if (buffer == null || pointer >= buffer.size()) {
            buffer = source.nextBatch();
        }
        while (buffer != null) {
            for (int i = pointer; i < buffer.size(); i++) {
                action.accept(buffer.get(i));
            }
            buffer = source.nextBatch();
        }
    }

    @Override
    public Spliterator<byte[]> trySplit() {
//        System.out.println("Thread: " + Thread.currentThread().getName()  + " trySplit");
        source.addDivisor();
        if (buffer == null || buffer.isEmpty()) {
            buffer = source.nextBatch();
        }
        if (buffer == null) {
            return null;
        }
        int otherBufferPoint = ((buffer.size() - pointer) / 2) + pointer;
        if (otherBufferPoint == pointer) {
            return null;
        }
        List<byte[]> otherBuffer = buffer.subList(otherBufferPoint, buffer.size());
        buffer = buffer.subList(pointer, otherBufferPoint);
        pointer = 0;
        return new RocksKeySpliterator(source, otherBuffer);
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return NONNULL | IMMUTABLE | DISTINCT;
    }


    static class BatchedSource {
        private int divisor = 1;
        private static final int BATCH_SIZE = 1024;

        BlockingQueue<List<byte[]>> batches = new LinkedBlockingQueue<>();

        private Supplier<RocksIterator> iteratorSupplier;
        private RocksIterator iterator;

        private ReentrantLock lock = new ReentrantLock();

        private volatile boolean ended = false;
        private volatile boolean closed = false;

        BatchedSource(Supplier<RocksIterator> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
        }

        public void addDivisor() {
            divisor++;
        }

        public List<byte[]> nextBatch() {
            List<byte[]> batch = batches.poll();
            if (batch != null) {
                return batch;
            }
            if (ended) {
                return null;
            }
            try {
                lock.lock();
                batch = batches.poll();
                if (batch != null) {
                    return batch;
                }
                if (ended) {
                    return null;
                }

                if (iterator == null) {
                    iterator = iteratorSupplier.get();
                }

                if (!iterator.isValid()) {
                    ended = true;
                    close();
                    return null;
                }

                List<byte[]> buffer = new ArrayList<>();
                for (int i = BATCH_SIZE; i >= 0; --i) {
                    buffer.add(iterator.key());
                    iterator.next();
                    if (!iterator.isValid()) {
                        ended = true;
                        close();
                        break;
                    }
                }

                int remaining = buffer.size();
                for (int j = divisor; j > 1; --j) {
                    int size = Math.min(remaining / j, 1);
                    int newRemaining = remaining - size;
                    if (newRemaining == 0) {
                        return buffer.subList(0, remaining);
                    } else {
                        batches.offer(buffer.subList(newRemaining, remaining));
                        remaining = newRemaining;
                    }
                }
                return buffer.subList(0, remaining);
            } finally {
                lock.unlock();
            }
        }

        private void close() {
            if (iterator != null && !closed) {
                iterator.close();
                closed = true;
            }
        }
    }
}