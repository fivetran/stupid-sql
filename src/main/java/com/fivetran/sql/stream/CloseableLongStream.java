package com.fivetran.sql.stream;

import java.util.*;
import java.util.function.*;
import java.util.stream.LongStream;

public class CloseableLongStream implements LongStream {
    private final LongStream delegate;
    private final AutoCloseable closeMe;

    public CloseableLongStream(LongStream delegate, AutoCloseable closeMe) {
        this.delegate = delegate;
        this.closeMe = closeMe;
    }

    @Override
    public CloseableLongStream filter(LongPredicate predicate) {
        return new CloseableLongStream(delegate.filter(predicate), closeMe);
    }

    @Override
    public CloseableLongStream map(LongUnaryOperator mapper) {
        return new CloseableLongStream(delegate.map(mapper), closeMe);
    }

    @Override
    public <U> CloseableStream<U> mapToObj(LongFunction<? extends U> mapper) {
        return new CloseableStream<>(delegate.mapToObj(mapper), closeMe);
    }

    @Override
    public CloseableIntStream mapToInt(LongToIntFunction mapper) {
        return new CloseableIntStream(delegate.mapToInt(mapper), closeMe);
    }

    @Override
    public CloseableDoubleStream mapToDouble(LongToDoubleFunction mapper) {
        return new CloseableDoubleStream(delegate.mapToDouble(mapper), closeMe);
    }

    @Override
    public CloseableLongStream flatMap(LongFunction<? extends LongStream> mapper) {
        return new CloseableLongStream(delegate.flatMap(mapper), closeMe);
    }

    @Override
    public CloseableLongStream distinct() {
        return new CloseableLongStream(delegate.distinct(), closeMe);
    }

    @Override
    public CloseableLongStream sorted() {
        return new CloseableLongStream(delegate.sorted(), closeMe);
    }

    @Override
    public CloseableLongStream peek(LongConsumer action) {
        return new CloseableLongStream(delegate.peek(action), closeMe);
    }

    @Override
    public CloseableLongStream limit(long maxSize) {
        return new CloseableLongStream(delegate.limit(maxSize), closeMe);
    }

    @Override
    public CloseableLongStream skip(long n) {
        return new CloseableLongStream(delegate.skip(n), closeMe);
    }

    @Override
    public void forEach(LongConsumer action) {
        delegate.forEach(action);
    }

    @Override
    public void forEachOrdered(LongConsumer action) {
        delegate.forEachOrdered(action);
    }

    @Override
    public long[] toArray() {
        return delegate.toArray();
    }

    @Override
    public long reduce(long identity, LongBinaryOperator op) {
        return delegate.reduce(identity, op);
    }

    @Override
    public OptionalLong reduce(LongBinaryOperator op) {
        return delegate.reduce(op);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        return delegate.collect(supplier, accumulator, combiner);
    }

    @Override
    public long sum() {
        return delegate.sum();
    }

    @Override
    public OptionalLong min() {
        return delegate.min();
    }

    @Override
    public OptionalLong max() {
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
    public LongSummaryStatistics summaryStatistics() {
        return delegate.summaryStatistics();
    }

    @Override
    public boolean anyMatch(LongPredicate predicate) {
        return delegate.anyMatch(predicate);
    }

    @Override
    public boolean allMatch(LongPredicate predicate) {
        return delegate.allMatch(predicate);
    }

    @Override
    public boolean noneMatch(LongPredicate predicate) {
        return delegate.noneMatch(predicate);
    }

    @Override
    public OptionalLong findFirst() {
        return delegate.findFirst();
    }

    @Override
    public OptionalLong findAny() {
        return delegate.findAny();
    }

    @Override
    public CloseableDoubleStream asDoubleStream() {
        return new CloseableDoubleStream(delegate.asDoubleStream(), closeMe);
    }

    @Override
    public CloseableStream<Long> boxed() {
        return new CloseableStream<>(delegate.boxed(), closeMe);
    }

    @Override
    public CloseableLongStream sequential() {
        return new CloseableLongStream(delegate.sequential(), closeMe);
    }

    @Override
    public CloseableLongStream parallel() {
        return new CloseableLongStream(delegate.parallel(), closeMe);
    }

    @Override
    public CloseableLongStream unordered() {
        return new CloseableLongStream(delegate.unordered(), closeMe);
    }

    @Override
    public CloseableLongStream onClose(Runnable closeHandler) {
        return new CloseableLongStream(delegate.unordered(), closeMe);
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
    public PrimitiveIterator.OfLong iterator() {
        return delegate.iterator();
    }

    @Override
    public Spliterator.OfLong spliterator() {
        return delegate.spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }
}
