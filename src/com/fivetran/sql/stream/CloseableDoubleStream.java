package com.fivetran.sql.stream;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.DoubleStream;

public class CloseableDoubleStream implements DoubleStream {
    private final DoubleStream delegate;
    private final AutoCloseable closeMe;

    public CloseableDoubleStream(DoubleStream delegate, AutoCloseable closeMe) {
        this.delegate = delegate;
        this.closeMe = closeMe;
    }

    @Override
    public CloseableDoubleStream filter(DoublePredicate predicate) {
        return new CloseableDoubleStream(delegate.filter(predicate), closeMe);
    }

    @Override
    public CloseableDoubleStream map(DoubleUnaryOperator mapper) {
        return new CloseableDoubleStream(delegate.map(mapper), closeMe);
    }

    @Override
    public <U> CloseableStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
        return new CloseableStream<>(delegate.mapToObj(mapper), closeMe);
    }

    @Override
    public CloseableIntStream mapToInt(DoubleToIntFunction mapper) {
        return new CloseableIntStream(delegate.mapToInt(mapper), closeMe);
    }

    @Override
    public CloseableLongStream mapToLong(DoubleToLongFunction mapper) {
        return new CloseableLongStream(delegate.mapToLong(mapper), closeMe);
    }

    @Override
    public CloseableDoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
        return new CloseableDoubleStream(delegate.flatMap(mapper), closeMe);
    }

    @Override
    public CloseableDoubleStream distinct() {
        return new CloseableDoubleStream(delegate.distinct(), closeMe);
    }

    @Override
    public CloseableDoubleStream sorted() {
        return new CloseableDoubleStream(delegate.sorted(), closeMe);
    }

    @Override
    public CloseableDoubleStream peek(DoubleConsumer action) {
        return new CloseableDoubleStream(delegate.peek(action), closeMe);
    }

    @Override
    public CloseableDoubleStream limit(long maxSize) {
        return new CloseableDoubleStream(delegate.limit(maxSize), closeMe);
    }

    @Override
    public CloseableDoubleStream skip(long n) {
        return new CloseableDoubleStream(delegate.skip(n), closeMe);
    }

    @Override
    public void forEach(DoubleConsumer action) {
        delegate.forEach(action);
    }

    @Override
    public void forEachOrdered(DoubleConsumer action) {
        delegate.forEachOrdered(action);
    }

    @Override
    public double[] toArray() {
        return delegate.toArray();
    }

    @Override
    public double reduce(double identity, DoubleBinaryOperator op) {
        return delegate.reduce(identity, op);
    }

    @Override
    public OptionalDouble reduce(DoubleBinaryOperator op) {
        return delegate.reduce(op);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
        return delegate.collect(supplier, accumulator, combiner);
    }

    @Override
    public double sum() {
        return delegate.sum();
    }

    @Override
    public OptionalDouble min() {
        return delegate.min();
    }

    @Override
    public OptionalDouble max() {
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
    public DoubleSummaryStatistics summaryStatistics() {
        return delegate.summaryStatistics();
    }

    @Override
    public boolean anyMatch(DoublePredicate predicate) {
        return delegate.anyMatch(predicate);
    }

    @Override
    public boolean allMatch(DoublePredicate predicate) {
        return delegate.allMatch(predicate);
    }

    @Override
    public boolean noneMatch(DoublePredicate predicate) {
        return delegate.noneMatch(predicate);
    }

    @Override
    public OptionalDouble findFirst() {
        return delegate.findFirst();
    }

    @Override
    public OptionalDouble findAny() {
        return delegate.findAny();
    }

    @Override
    public CloseableStream<Double> boxed() {
        return new CloseableStream<>(delegate.boxed(), closeMe);
    }

    @Override
    public CloseableDoubleStream sequential() {
        return new CloseableDoubleStream(delegate.sequential(), closeMe);
    }

    @Override
    public CloseableDoubleStream parallel() {
        return new CloseableDoubleStream(delegate.parallel(), closeMe);
    }

    @Override
    public CloseableDoubleStream unordered() {
        return new CloseableDoubleStream(delegate.unordered(), closeMe);
    }

    @Override
    public CloseableDoubleStream onClose(Runnable closeHandler) {
        return new CloseableDoubleStream(delegate.unordered(), closeMe);
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
    public PrimitiveIterator.OfDouble iterator() {
        return delegate.iterator();
    }

    @Override
    public Spliterator.OfDouble spliterator() {
        return delegate.spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }
}
