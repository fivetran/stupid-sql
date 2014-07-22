package sql;

import java.util.*;
import java.util.function.*;
import java.util.stream.IntStream;

public class CloseableIntStream implements IntStream {
    private final IntStream delegate;
    private final AutoCloseable closeMe;

    public CloseableIntStream(IntStream delegate, AutoCloseable closeMe) {
        this.delegate = delegate;
        this.closeMe = closeMe;
    }

    @Override
    public CloseableIntStream filter(IntPredicate predicate) {
        return new CloseableIntStream(delegate.filter(predicate), closeMe);
    }

    @Override
    public CloseableIntStream map(IntUnaryOperator mapper) {
        return new CloseableIntStream(delegate.map(mapper), closeMe);
    }

    @Override
    public <U> CloseableStream<U> mapToObj(IntFunction<? extends U> mapper) {
        return new CloseableStream<>(delegate.mapToObj(mapper), closeMe);
    }

    @Override
    public CloseableLongStream mapToLong(IntToLongFunction mapper) {
        return new CloseableLongStream(delegate.mapToLong(mapper), closeMe);
    }

    @Override
    public CloseableDoubleStream mapToDouble(IntToDoubleFunction mapper) {
        return new CloseableDoubleStream(delegate.mapToDouble(mapper), closeMe);
    }

    @Override
    public CloseableIntStream flatMap(IntFunction<? extends IntStream> mapper) {
        return new CloseableIntStream(delegate.flatMap(mapper), closeMe);
    }

    @Override
    public CloseableIntStream distinct() {
        return new CloseableIntStream(delegate.distinct(), closeMe);
    }

    @Override
    public CloseableIntStream sorted() {
        return new CloseableIntStream(delegate.sorted(), closeMe);
    }

    @Override
    public CloseableIntStream peek(IntConsumer action) {
        return new CloseableIntStream(delegate.peek(action), closeMe);
    }

    @Override
    public CloseableIntStream limit(long maxSize) {
        return new CloseableIntStream(delegate.limit(maxSize), closeMe);
    }

    @Override
    public CloseableIntStream skip(long n) {
        return new CloseableIntStream(delegate.skip(n), closeMe);
    }

    @Override
    public void forEach(IntConsumer action) {
        delegate.forEach(action);
    }

    @Override
    public void forEachOrdered(IntConsumer action) {
        delegate.forEachOrdered(action);
    }

    @Override
    public int[] toArray() {
        return delegate.toArray();
    }

    @Override
    public int reduce(int identity, IntBinaryOperator op) {
        return delegate.reduce(identity, op);
    }

    @Override
    public OptionalInt reduce(IntBinaryOperator op) {
        return delegate.reduce(op);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        return delegate.collect(supplier, accumulator, combiner);
    }

    @Override
    public int sum() {
        return delegate.sum();
    }

    @Override
    public OptionalInt min() {
        return delegate.min();
    }

    @Override
    public OptionalInt max() {
        return delegate.max();
    }

    @Override
    public long count() {
        return delegate.count();
    }

    @Override
    public OptionalDouble average() {
        return delegate.average();
    }

    @Override
    public IntSummaryStatistics summaryStatistics() {
        return delegate.summaryStatistics();
    }

    @Override
    public boolean anyMatch(IntPredicate predicate) {
        return delegate.anyMatch(predicate);
    }

    @Override
    public boolean allMatch(IntPredicate predicate) {
        return delegate.allMatch(predicate);
    }

    @Override
    public boolean noneMatch(IntPredicate predicate) {
        return delegate.noneMatch(predicate);
    }

    @Override
    public OptionalInt findFirst() {
        return delegate.findFirst();
    }

    @Override
    public OptionalInt findAny() {
        return delegate.findAny();
    }

    @Override
    public CloseableLongStream asLongStream() {
        return new CloseableLongStream(delegate.asLongStream(), closeMe);
    }

    @Override
    public CloseableDoubleStream asDoubleStream() {
        return new CloseableDoubleStream(delegate.asDoubleStream(), closeMe);
    }

    @Override
    public CloseableStream<Integer> boxed() {
        return new CloseableStream<>(delegate.boxed(), closeMe);
    }

    @Override
    public CloseableIntStream sequential() {
        return new CloseableIntStream(delegate.sequential(), closeMe);
    }

    @Override
    public CloseableIntStream parallel() {
        return new CloseableIntStream(delegate.parallel(), closeMe);
    }

    @Override
    public CloseableIntStream unordered() {
        return new CloseableIntStream(delegate.unordered(), closeMe);
    }

    @Override
    public CloseableIntStream onClose(Runnable closeHandler) {
        return new CloseableIntStream(delegate.unordered(), closeMe);
    }

    @Override
    public void close() {
        try {
            closeMe.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            delegate.close();
        }
    }

    @Override
    public PrimitiveIterator.OfInt iterator() {
        return delegate.iterator();
    }

    @Override
    public Spliterator.OfInt spliterator() {
        return delegate.spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }
}
