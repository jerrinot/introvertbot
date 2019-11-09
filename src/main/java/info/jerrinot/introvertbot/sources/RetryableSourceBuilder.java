/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.jerrinot.introvertbot.sources;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.core.processor.SourceProcessors.convenientSourceP;
import static com.hazelcast.jet.core.processor.SourceProcessors.convenientTimestampedSourceP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Top-level class for Jet custom source builders. It is associated
 * with a <em>context object</em> that holds any necessary state and
 * resources you need to make your source work.
 * <p>
 * For further details refer to the factory methods:
 * <ul>
 *     <li>{@link #batch(String, FunctionEx)}
 *     <li>{@link #timestampedStream(String, FunctionEx)}
 *     <li>{@link #stream(String, FunctionEx)}
 * </ul>
 *
 * @param <C> type of the context object
 *
 * @since 3.0
 */
public final class RetryableSourceBuilder<C> {
    private final String name;
    private final FunctionEx<? super Context, ? extends C> createFn;
    private FunctionEx<? super C, Object> createSnapshotFn = ctx -> null;
    private BiConsumerEx<? super C, ? super List<Object>> restoreSnapshotFn = (ctx, states) -> { };
    private ConsumerEx<? super C> destroyFn = ConsumerEx.noop();
    private int preferredLocalParallelism;

    private RetryableSourceBuilder(
            @Nonnull String name,
            @Nonnull FunctionEx<? super Context, ? extends C> createFn
    ) {
        checkSerializable(createFn, "createFn");
        this.name = name;
        this.createFn = createFn;
    }

    /**
     * Returns a fluent-API builder with which you can create a {@linkplain
     * BatchSource batch source} for a Jet pipeline. The source will use
     * {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel processor that drives your source has its private instance
     * of a <i>context object</i> it got from your {@code createFn}. To get
     * the data items to emit to the pipeline, the processor repeatedly calls
     * your {@code fillBufferFn} with the context object and a buffer object.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. A hundred items at a time is enough to
     * eliminate any per-call overheads within Jet. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Once it has emitted all the data, {@code fillBufferFn} must call {@link
     * SourceBuffer#close() buffer.close()}. This signals Jet to not call {@code
     * fillBufferFn} again and at some later point it will call the {@code
     * destroyFn} with the context object.
     * <p>
     * Unless you call {@link RetryableSourceBuilder.Batch#distributed builder.distributed()},
     * Jet will create just a single processor that should emit all the data.
     * If you do call it, make sure your distributed source takes care of
     * splitting the data between processors. Your {@code createFn} should
     * consult {@link Context#totalParallelism() processorContext.totalParallelism()}
     * and {@link Context#globalProcessorIndex() processorContext.globalProcessorIndex()}.
     * Jet calls it exactly once with each {@code globalProcessorIndex} from 0
     * to {@code totalParallelism - 1} and each of the resulting context
     * objects must emit its distinct slice of the total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * reads the lines from a single text file. Since you can't control on
     * which member of the Jet cluster the source's processor will run, the
     * file should be available on all members. The source emits one line per
     * {@code fillBufferFn} call.
     * <pre>{@code
     * BatchSource<String> fileSource = MySourceBuilder
     *         .batch("file-source", processorContext ->
     *             new BufferedReader(new FileReader("input.txt")))
     *         .<String>fillBufferFn((in, buf) -> {
     *             String line = in.readLine();
     *             if (line != null) {
     *                 buf.add(line);
     *             } else {
     *                 buf.close();
     *             }
     *         })
     *         .destroyFn(BufferedReader::close)
     *         .build();
     * Pipeline p = Pipeline.create();
     * BatchStage<String> srcStage = p.drawFrom(fileSource);
     * }</pre>
     *
     * @param name     a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the source's context object
     * @param <C>      type of the context object
     *
     * @since 3.0
     */
    @Nonnull
    public static <C> RetryableSourceBuilder<C>.Batch<Void> batch(
            @Nonnull String name,
            @Nonnull FunctionEx<? super Processor.Context, ? extends C> createFn
    ) {
        return new RetryableSourceBuilder<C>(name, createFn).new Batch<Void>();
    }

    /**
     * Returns a fluent-API builder with which you can create an {@linkplain
     * StreamSource unbounded stream source} for a Jet pipeline. The source will
     * use {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * Each parallel processor that drives your source has its private instance
     * of a <i>context object</i> it got from your {@code createFn}. To get
     * the data items to emit to the pipeline, the processor repeatedly calls
     * your {@code fillBufferFn} with the state object and a buffer object.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. A hundred items at a time is enough to
     * eliminate any per-call overheads within Jet. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Unless you call {@link RetryableSourceBuilder.Stream#distributed builder.distributed()},
     * Jet will create just a single processor that should emit all the data.
     * If you do call it, make sure your distributed source takes care of
     * splitting the data between processors. Your {@code createFn} should
     * consult {@link Context#totalParallelism() processorContext.totalParallelism()}
     * and {@link Context#globalProcessorIndex() processorContext.globalProcessorIndex()}.
     * Jet calls it exactly once with each {@code globalProcessorIndex} from 0
     * to {@code totalParallelism - 1} and each of the resulting context objects
     * must emit its distinct slice of the total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * polls a URL and emits all the lines it gets in the response:
     * <pre>{@code
     * StreamSource<String> socketSource = MySourceBuilder
     *     .stream("http-source", processorContext -> HttpClients.createDefault())
     *     .<String>fillBufferFn((httpc, buf) -> {
     *         new BufferedReader(new InputStreamReader(
     *             httpc.execute(new HttpGet("localhost:8008"))
     *                  .getEntity().getContent()))
     *             .lines()
     *             .forEach(buf::add);
     *     })
     *     .destroyFn(Closeable::close)
     *     .build();
     * Pipeline p = Pipeline.create();
     * StreamStage<String> srcStage = p.drawFrom(socketSource);
     * }</pre>
     *
     * @param name     a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the source's context object
     * @param <C>      type of the context object
     *
     * @since 3.0
     */
    @Nonnull
    public static <C> RetryableSourceBuilder<C>.Stream<Void> stream(
            @Nonnull String name,
            @Nonnull FunctionEx<? super Processor.Context, ? extends C> createFn
    ) {
        return new RetryableSourceBuilder<C>(name, createFn).new Stream<Void>();
    }

    /**
     * Returns a fluent-API builder with which you can create an {@linkplain
     * StreamSource unbounded stream source} for a Jet pipeline. It will use
     * {@linkplain Processor#isCooperative() non-cooperative} processors.
     * <p>
     * When emitting an item, the source can explicitly assign a timestamp to
     * it. You can tell a Jet pipeline to use those timestamps by calling
     * {@link StreamSourceStage#withNativeTimestamps sourceStage.withNativeTimestamps()}.
     * <p>
     * Each parallel processor that drives your source has its private instance
     * of a <i>context object</i> it gets from the given {@code createFn}. To get
     * the data items to emit to the pipeline, the processor repeatedly calls
     * your {@code fillBufferFn} with the context object and a buffer object. The
     * buffer's {@link RetryableSourceBuilder.TimestampedSourceBuffer#add add()} method
     * takes two arguments: the item and the timestamp in milliseconds.
     * <p>
     * Your function should add some items to the buffer, ideally those it has
     * ready without having to block. A hundred items at a time is enough to
     * eliminate any per-call overheads within Jet. If it doesn't have any
     * items ready, it may also return without adding anything. In any case the
     * function should not take more than a second or so to complete, otherwise
     * you risk interfering with Jet's coordination mechanisms and getting bad
     * performance.
     * <p>
     * Unless you call {@link RetryableSourceBuilder.TimestampedStream#distributed(int)
     * builder.distributed()}, Jet will create just a single processor that
     * should emit all the data. If you do call it, make sure your distributed
     * source takes care of splitting the data between processors. Your {@code
     * createFn} should consult {@link Context#totalParallelism()
     * procContext.totalParallelism()} and {@link Context#globalProcessorIndex()
     * procContext.globalProcessorIndex()}. Jet calls it exactly once with each
     * {@code globalProcessorIndex} from 0 to {@code totalParallelism - 1} and
     * each of the resulting context objects must emit its distinct slice of the
     * total source data.
     * <p>
     * Here's an example that builds a simple, non-distributed source that
     * polls a URL and emits all the lines it gets in the response,
     * interpreting the first 9 characters as the timestamp.
     * <pre>{@code
     * StreamSource<String> socketSource = MySourceBuilder
     *     .timestampedStream("http-source",
     *         processorContext -> HttpClients.createDefault())
     *     .<String>fillBufferFn((httpc, buf) -> {
     *         new BufferedReader(new InputStreamReader(
     *             httpc.execute(new HttpGet("localhost:8008"))
     *                  .getEntity().getContent()))
     *             .lines()
     *             .forEach(line -> {
     *                 long timestamp = Long.valueOf(line.substring(0, 9));
     *                 buf.add(line.substring(9), timestamp);
     *             });
     *     })
     *     .destroyFn(Closeable::close)
     *     .build();
     * Pipeline p = Pipeline.create();
     * StreamStage<String> srcStage = p.drawFrom(socketSource)
     *         .withNativeTimestamps(SECONDS.toMillis(5));
     * }</pre>
     * <p>
     * <strong>NOTE:</strong> if the data source you're adapting to Jet is
     * partitioned, you may run into issues with event skew between partitions
     * assigned to a given parallel processor. The timestamp you get from one
     * partition may be significantly behind the timestamp you already got from
     * another one. If the skew is more than the allowed lag and you have
     * {@linkplain StreamSourceStage#withNativeTimestamps(long) configured
     * native timestamps}, you risk that the events will be late. Use a
     * {@linkplain Sources#streamFromProcessorWithWatermarks custom processor}
     * if you need to coalesce watermarks from multiple partitions.
     *
     * @param name a descriptive name for the source (for diagnostic purposes)
     * @param createFn a function that creates the source's context object
     * @param <C> type of the context object
     *
     * @since 3.0
     */
    @Nonnull
    public static <C> RetryableSourceBuilder<C>.TimestampedStream<Void> timestampedStream(
            @Nonnull String name,
            @Nonnull FunctionEx<? super Processor.Context, ? extends C> createFn
    ) {
        return new RetryableSourceBuilder<C>(name, createFn).new TimestampedStream<Void>();
    }

    private abstract class Base<T> {
        private Base() {
        }

        /**
         * Sets the function that Jet will call when it is done cleaning up after
         * an execution. It gives you the opportunity to release any resources that
         * your context object may be holding. Jet also calls this function when
         * the user cancels or restarts the job.
         */
        @Nonnull
        public Base<T> destroyFn(@Nonnull ConsumerEx<? super C> destroyFn) {
            checkSerializable(destroyFn, "destroyFn");
            RetryableSourceBuilder.this.destroyFn = destroyFn;
            return this;
        }

        /**
         * Declares that you're creating a distributed source. On each member of
         * the cluster Jet will create as many processors as you specify with the
         * {@code preferredLocalParallelism} parameter. If you call this, you must
         * ensure that all the source processors are coordinated and not emitting
         * duplicated data. The {@code createFn} can consult {@link
         * Processor.Context#totalParallelism processorContext.totalParallelism()}
         * and {@link Processor.Context#globalProcessorIndex
         * processorContext.globalProcessorIndex()}. Jet calls {@code createFn}
         * exactly once with each {@code globalProcessorIndex} from 0 to {@code
         * totalParallelism - 1} and you can use this to make all the instances
         * agree on which part of the data to emit.
         * <p>
         * If you don't call this method, there will be only one processor instance
         * running on an arbitrary member.
         *
         * @param preferredLocalParallelism the requested number of processors on each cluster member
         */
        @Nonnull
        public Base<T> distributed(int preferredLocalParallelism) {
            checkPositive(preferredLocalParallelism, "Preferred local parallelism must >= 1");
            RetryableSourceBuilder.this.preferredLocalParallelism = preferredLocalParallelism;
            return this;
        }

        /**
         * Sets the function Jet calls when it's creating a snapshot of the
         * current job state. This happens in all Jet jobs that have a {@linkplain
         * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
         * guarantee} configured.
         * <p>
         * When Jet restarts a job, it first initializes your source as if starting
         * a new job, and then passes the snapshot object you returned here to
         * {@link FaultTolerant#restoreSnapshotFn restoreSnapshotFn}. After that it
         * starts calling {@code fillBufferFn}, which must resume emitting the
         * stream from the same item it was about to emit when the snapshot was
         * taken.
         * <p>
         * The object you return must be serializable. Each source processor will
         * call the function once per snapshot.
         * <p>
         * Here's an example of a fault-tolerant generator of an infinite sequence of
         * integers:
         * <pre>{@code
         * StreamSource<Integer> source = MySourceBuilder
         *         .stream("name", processorContext -> new AtomicInteger())
         *         .<Integer>fillBufferFn((numToEmit, buffer) -> {
         *             for (int i = 0; i < 100; i++) {
         *                 buffer.add(numToEmit.getAndIncrement());
         *             }
         *         })
         *         .createSnapshotFn(numToEmit -> numToEmit.get())
         *         .restoreSnapshotFn((numToEmit, states) -> numToEmit.set(states.get(0)))
         *         .build();
         * }</pre>
         *
         * @param <S> type of the snapshot object
         *
         * @since 3.1
         */
        @Nonnull
        abstract <S> FaultTolerant<? extends Base<T>, S> createSnapshotFn(
                @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn
        );
    }

    private abstract class BaseNoTimestamps<T> extends Base<T> {
        BiConsumerEx<? super C, ? super SourceBuilder.SourceBuffer<T>> fillBufferFn;

        private BaseNoTimestamps() {
        }

        /**
         * Sets the function that Jet will call whenever it needs more data from
         * your source. The function receives the context object obtained from
         * {@code createFn} and Jet's buffer object. It should add some items
         * to the buffer, ideally those it can produce without making any blocking
         * calls. On any given invocation the function may also choose not to add
         * any items. Jet will automatically employ an exponential backoff strategy
         * to avoid calling your function in a tight loop, if the previous call didn't
         * add any items to the buffer.
         *
         * @param fillBufferFn function that fills the buffer with source data
         * @param <T_NEW> type of the emitted items
         * @return this builder with the item type reset to the one inferred from
         *         {@code fillBufferFn}
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> BaseNoTimestamps<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuilder.SourceBuffer<T_NEW>> fillBufferFn
        ) {
            checkSerializable(fillBufferFn, "fillBufferFn");
            BaseNoTimestamps<T_NEW> newThis = (BaseNoTimestamps<T_NEW>) this;
            newThis.fillBufferFn = fillBufferFn;
            return newThis;
        }
    }

    /**
     * See {@link RetryableSourceBuilder#batch(String, FunctionEx)}.
     *
     * @param <T> type of emitted objects
     *
     * @since 3.0
     */
    public final class Batch<T> extends BaseNoTimestamps<T> {
        private Batch() {
        }

        /**
         * {@inheritDoc}
         * <p>
         * Once it has emitted all the data, the function must call {@link
         * SourceBuffer#close}.
         */
        @Override @Nonnull
        public <T_NEW> RetryableSourceBuilder<C>.Batch<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuilder.SourceBuffer<T_NEW>> fillBufferFn
        ) {
            return (Batch<T_NEW>) super.fillBufferFn(fillBufferFn);
        }

        @Override @Nonnull
        public Batch<T> destroyFn(@Nonnull ConsumerEx<? super C> destroyFn) {
            return (Batch<T>) super.destroyFn(destroyFn);
        }

        @Override @Nonnull
        public Batch<T> distributed(int preferredLocalParallelism) {
            return (Batch<T>) super.distributed(preferredLocalParallelism);
        }

        /**
         * Builds and returns the batch source.
         */
        @Nonnull
        public BatchSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn must be non-null");
            return new BatchSourceTransform<>(name, convenientSourceP(createFn, fillBufferFn, createSnapshotFn,
                    restoreSnapshotFn, destroyFn, preferredLocalParallelism, true));
        }

        /**
         * A private method. Do not call.
         */
        @Nonnull @Override
        @SuppressWarnings("unchecked")
        FaultTolerant createSnapshotFn(@Nonnull FunctionEx createSnapshotFn) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * See {@link RetryableSourceBuilder#stream(String, FunctionEx)}.
     *
     * @param <T> type of emitted objects
     *
     * @since 3.0
     */
    public final class Stream<T> extends BaseNoTimestamps<T> {
        private Stream() {
        }

        @Override @Nonnull
        public <T_NEW> Stream<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuilder.SourceBuffer<T_NEW>> fillBufferFn
        ) {
            return (Stream<T_NEW>) super.fillBufferFn(fillBufferFn);
        }

        @Override @Nonnull
        public Stream<T> destroyFn(@Nonnull ConsumerEx<? super C> pDestroyFn) {
            return (Stream<T>) super.destroyFn(pDestroyFn);
        }

        @Override @Nonnull
        public Stream<T> distributed(int preferredLocalParallelism) {
            return (Stream<T>) super.distributed(preferredLocalParallelism);
        }

        @Override @Nonnull
        public <S> FaultTolerant<Stream<T>, S> createSnapshotFn(
                @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn
        ) {
            return new FaultTolerant<>(this, createSnapshotFn);
        }

        /**
         * Builds and returns the unbounded stream source.
         */
        @Nonnull
        public StreamSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn() wasn't called");
            return new StreamSourceTransform<>(
                    name,
                    eventTimePolicy -> convenientSourceP(
                            createFn, fillBufferFn, createSnapshotFn, restoreSnapshotFn,
                            destroyFn, preferredLocalParallelism, false),
                    false, false);
        }
    }

    /**
     * See {@link RetryableSourceBuilder#timestampedStream(String, FunctionEx)}.
     *
     * @param <T> type of emitted objects
     *
     * @since 3.0
     */
    public final class TimestampedStream<T> extends Base<T> {
        private BiConsumerEx<? super C, ? super SourceBuilder.TimestampedSourceBuffer<T>> fillBufferFn;
        private TriFunction<? super C, Throwable, Long, ErrorOutcome> errorFn;

        private TimestampedStream() {
        }

        /**
         * Sets the function that Jet will call whenever it needs more data from
         * your source. The function receives the context object obtained from
         * {@code createFn} and Jet's buffer object. It should add some items
         * to the buffer, ideally those it can produce without making any blocking
         * calls. The buffer's {@link RetryableSourceBuilder.TimestampedSourceBuffer#add add()}
         * method takes two arguments: the item and the timestamp in milliseconds.
         * <p>
         * On any given invocation the function may also choose not to add
         * any items. Jet will automatically employ an exponential backoff strategy
         * to avoid calling your function in a tight loop, if the previous call didn't
         * add any items to the buffer.
         *
         * @param fillBufferFn function that fills the buffer with source data
         * @param <T_NEW> type of the emitted items
         * @return this builder with the item type reset to the one inferred from
         *         {@code fillBufferFn}
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> TimestampedStream<T_NEW> fillBufferFn(
                @Nonnull BiConsumerEx<? super C, ? super SourceBuilder.TimestampedSourceBuffer<T_NEW>> fillBufferFn
        ) {
            TimestampedStream<T_NEW> newThis = (TimestampedStream<T_NEW>) this;
            newThis.fillBufferFn = fillBufferFn;
            return newThis;
        }

        @Override @Nonnull
        public TimestampedStream<T> destroyFn(@Nonnull ConsumerEx<? super C> pDestroyFn) {
            return (TimestampedStream<T>) super.destroyFn(pDestroyFn);
        }

        @Nonnull
        public TimestampedStream<T> errorFn(@Nonnull TriFunction<? super C, Throwable, Long, ErrorOutcome> errorFn) {
            this.errorFn = errorFn;
            return this;
        }

        @Override @Nonnull
        public TimestampedStream<T> distributed(int preferredLocalParallelism) {
            return (TimestampedStream<T>) super.distributed(preferredLocalParallelism);
        }

        @Override @Nonnull
        public <S> FaultTolerant<TimestampedStream<T>, S> createSnapshotFn(
                @Nonnull FunctionEx<? super C, ? extends S> createSnapshotFn
        ) {
            return new FaultTolerant<>(this, createSnapshotFn);
        }

        /**
         * Builds and returns the timestamped stream source.
         */
        @Nonnull
        public StreamSource<T> build() {
            Preconditions.checkNotNull(fillBufferFn, "fillBufferFn must be set");

            if (errorFn == null) {
                return new StreamSourceTransform<>(
                        name,
                        eventTimePolicy -> convenientTimestampedSourceP(createFn, fillBufferFn, eventTimePolicy,
                                createSnapshotFn, restoreSnapshotFn, destroyFn, preferredLocalParallelism),
                        true, true);
            }

            // make local copy of fields to prevent capturing enclosing object ('this')
            BiConsumerEx<? super C, ? super SourceBuilder.TimestampedSourceBuffer<T>> fillBufferFn = this.fillBufferFn;
            FunctionEx<? super Context, ? extends C> createFn = RetryableSourceBuilder.this.createFn;
            ConsumerEx<? super C> destroyFn = RetryableSourceBuilder.this.destroyFn;
            FunctionEx<? super C, Object> createSnapshotFn = RetryableSourceBuilder.this.createSnapshotFn;
            BiConsumerEx<? super C, ? super List<Object>> restoreSnapshotFn = RetryableSourceBuilder.this.restoreSnapshotFn;
            TriFunction<? super C, Throwable, Long, ErrorOutcome> errorFn = this.errorFn;

            return new StreamSourceTransform<>(
                    name,
                    eventTimePolicy -> convenientTimestampedSourceP(c -> new RetryableContext<>(c, createFn, fillBufferFn, destroyFn, createSnapshotFn, restoreSnapshotFn, errorFn),
                            RetryableContext::fill, eventTimePolicy, RetryableContext::createSnapshot, RetryableContext::restoreSnapshot, RetryableContext::destroy, preferredLocalParallelism),
                    true, true);
        }
    }

    /**
     * Represents a step in building a custom source where you add a {@link
     * #restoreSnapshotFn} after adding a {@link Base#createSnapshotFn
     * createSnapshotFn}.
     *
     * @param <B> type of the builder this sub-builder was created from
     * @param <S> type of the object saved to the state snapshot
     *
     * @since 3.1
     */
    public final class FaultTolerant<B, S> {
        private final B parentBuilder;

        @SuppressWarnings("unchecked")
        private FaultTolerant(B parentBuilder, FunctionEx<? super C, ? extends S> createSnapshotFn) {
            checkSerializable(createSnapshotFn, "createSnapshotFn");
            this.parentBuilder = parentBuilder;
            RetryableSourceBuilder.this.createSnapshotFn = (FunctionEx<? super C, Object>) createSnapshotFn;
        }

        /**
         * Sets the function that restores the source's state from a snapshot.
         * <p>
         * When Jet is restarting a job after it was interrupted (failure or other
         * reasons), it first initializes your source as if starting a new job and
         * then passes the snapshot object (the one it got from your {@link
         * Base#createSnapshotFn createSnapshotFn}) to this function. Then it
         * starts calling {@code fillBufferFn}, which must resume emitting the
         * stream from the same item it was about to emit when the snapshot was
         * taken.
         * <p>
         * If your source is <em>not {@linkplain Base#distributed(int)
         * distributed}</em>, the `List` in the second argument contains
         * exactly 1 element; it is safe to use `get(0)` on it. If your source
         * is distributed, the list will contain objects returned by {@code
         * createSnapshotFn} in all parallel instances. This is why {@code
         * restoreSnapshotFn} accepts a list of snapshot objects. It should
         * figure out which part of the snapshot data pertains to it and it can
         * do so as explained {@link Base#distributed here}.
         *
         * @since 3.1
         */
        @SuppressWarnings("unchecked")
        @Nonnull
        public B restoreSnapshotFn(@Nonnull BiConsumerEx<? super C, ? super List<S>> restoreSnapshotFn) {
            checkSerializable(restoreSnapshotFn, "restoreSnapshotFn");
            RetryableSourceBuilder.this.restoreSnapshotFn = (BiConsumerEx<? super C, ? super List<Object>>) restoreSnapshotFn;
            return parentBuilder;
        }
    }
}