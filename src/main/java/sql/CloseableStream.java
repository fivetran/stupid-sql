package sql;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.*;

class CloseableStream<T> implements Stream<T> {

    private final Stream<T> delegate;
    private final AutoCloseable closeMe;

    CloseableStream(Stream<T> delegate, AutoCloseable closeMe) {
        this.delegate = delegate;
        this.closeMe = closeMe;
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return delegate.spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate.isParallel();
    }

    @Override
    public CloseableStream<T> sequential() {
        return new CloseableStream<>(delegate.sequential(), closeMe);
    }

    @Override
    public CloseableStream<T> parallel() {
        return new CloseableStream<>(delegate.parallel(), closeMe);
    }

    @Override
    public CloseableStream<T> unordered() {
        return new CloseableStream<>(delegate.unordered(), closeMe);
    }

    @Override
    public CloseableStream<T> onClose(Runnable closeHandler) {
        return new CloseableStream<>(delegate.onClose(closeHandler), closeMe);
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
    public CloseableStream<T> filter(Predicate<? super T> predicate) {
        return new CloseableStream<>(delegate.filter(predicate), closeMe);
    }

    @Override
    public <R> CloseableStream<R> map(Function<? super T, ? extends R> mapper) {
        return new CloseableStream<>(delegate.map(mapper), closeMe);
    }

    @Override
    public CloseableIntStream mapToInt(ToIntFunction<? super T> mapper) {
        return new CloseableIntStream(delegate.mapToInt(mapper), closeMe);
    }

    @Override
    public CloseableLongStream mapToLong(ToLongFunction<? super T> mapper) {
        return new CloseableLongStream(delegate.mapToLong(mapper), closeMe);
    }

    @Override
    public CloseableDoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return new CloseableDoubleStream(delegate.mapToDouble(mapper), closeMe);
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new CloseableStream<>(delegate.flatMap(mapper), closeMe);
    }

    @Override
    public CloseableIntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return new CloseableIntStream(delegate.flatMapToInt(mapper), closeMe);
    }

    @Override
    public CloseableLongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return new CloseableLongStream(delegate.flatMapToLong(mapper), closeMe);
    }

    @Override
    public CloseableDoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return new CloseableDoubleStream(delegate.flatMapToDouble(mapper), closeMe);
    }

    @Override
    public CloseableStream<T> distinct() {
        return new CloseableStream<>(delegate.distinct(), closeMe);
    }

    @Override
    public CloseableStream<T> sorted() {
        return new CloseableStream<>(delegate.sorted(), closeMe);
    }

    @Override
    public CloseableStream<T> sorted(Comparator<? super T> comparator) {
        return new CloseableStream<>(delegate.sorted(comparator), closeMe);
    }

    @Override
    public CloseableStream<T> peek(Consumer<? super T> action) {
        return new CloseableStream<>(delegate.peek(action), closeMe);
    }

    @Override
    public CloseableStream<T> limit(long maxSize) {
        return new CloseableStream<>(delegate.limit(maxSize), closeMe);
    }

    @Override
    public CloseableStream<T> skip(long n) {
        return new CloseableStream<>(delegate.skip(n), closeMe);
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        delegate.forEach(action);
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        delegate.forEachOrdered(action);
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return delegate.toArray(generator);
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        return delegate.reduce(identity, accumulator);
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return delegate.reduce(accumulator);
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return delegate.reduce(identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return delegate.collect(supplier, accumulator, combiner);
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return delegate.collect(collector);
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        return delegate.min(comparator);
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        return delegate.max(comparator);
    }

    @Override
    public long count() {
        return delegate.count();
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        return delegate.anyMatch(predicate);
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        return delegate.allMatch(predicate);
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        return delegate.noneMatch(predicate);
    }

    @Override
    public Optional<T> findFirst() {
        return delegate.findFirst();
    }

    @Override
    public Optional<T> findAny() {
        return delegate.findAny();
    }
}
