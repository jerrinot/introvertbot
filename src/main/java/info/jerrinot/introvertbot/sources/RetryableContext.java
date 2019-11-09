package info.jerrinot.introvertbot.sources;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.io.Serializable;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class RetryableContext<INNER_CONTEXT_TYPE, ITEM_TYPE, SNAPSHOT_TYPE> implements Serializable {
    private static final long BACKOFF_NANOS = MILLISECONDS.toNanos(100);

    private INNER_CONTEXT_TYPE innerContext;
    private final BiConsumerEx<? super INNER_CONTEXT_TYPE, ? super SourceBuilder.TimestampedSourceBuffer<ITEM_TYPE>> innerFillBufferFn;
    private final ConsumerEx<? super INNER_CONTEXT_TYPE> innerDestroyFn;
    private final FunctionEx<? super INNER_CONTEXT_TYPE, ? extends SNAPSHOT_TYPE> createSnapshotFn;
    private final BiConsumerEx<? super INNER_CONTEXT_TYPE, ? super List<Object>> restoreSnapshotFn;
    private final Processor.Context processorContext;
    private final FunctionEx<? super Processor.Context, ? extends INNER_CONTEXT_TYPE> createFn;
    private final TriFunction<? super INNER_CONTEXT_TYPE, Throwable, Long, ErrorOutcome> errorFn;
    private long lastErrorAtNanos;
    private long firstErrorAtNanos;

    public RetryableContext(Processor.Context processorContext,
                            FunctionEx<? super Processor.Context, ? extends INNER_CONTEXT_TYPE> createFn,
                            BiConsumerEx<? super INNER_CONTEXT_TYPE, ? super SourceBuilder.TimestampedSourceBuffer<ITEM_TYPE>> fillBufferFn,
                            ConsumerEx<? super INNER_CONTEXT_TYPE> destroyFn,
                            FunctionEx<? super INNER_CONTEXT_TYPE, ? extends SNAPSHOT_TYPE> createSnapshotFn,
                            BiConsumerEx<? super INNER_CONTEXT_TYPE, ? super List<Object>> restoreSnapshotFn,
                            TriFunction<? super INNER_CONTEXT_TYPE, Throwable, Long, ErrorOutcome> errorFn) {
        this.innerFillBufferFn = fillBufferFn;
        this.innerDestroyFn = destroyFn;
        this.createSnapshotFn = createSnapshotFn;
        this.restoreSnapshotFn = restoreSnapshotFn;
        this.processorContext = processorContext;
        this.createFn = createFn;
        this.errorFn = errorFn;
    }

    public <T extends SourceBuilder.TimestampedSourceBuffer<ITEM_TYPE>> void fill(T buffer) {
        long now = System.nanoTime();
        try {
            if (lastErrorAtNanos != 0) {
                if (now < lastErrorAtNanos + BACKOFF_NANOS) {
                    return;
                }
            }
            if (innerContext == null) {
                innerContext = createFn.apply(processorContext);
            }
            innerFillBufferFn.accept(innerContext, buffer);
            lastErrorAtNanos = 0;
            firstErrorAtNanos = 0;
        } catch (Throwable e) {
            if (firstErrorAtNanos == 0) {
                firstErrorAtNanos = now;
            }
            long errorDurationMillis = NANOSECONDS.toMillis(now - firstErrorAtNanos);
            ErrorOutcome outcome = errorFn.apply(innerContext, e, errorDurationMillis);
            switch (outcome) {
                case PROPAGATE_ERROR:
                    throw e;
                case RECREATE_CONTEXT:
                    innerContext = null;
                    // intentional fall-through
                case BACKOFF:
                    lastErrorAtNanos = System.nanoTime();
                    break;
                default:
                    throw new IllegalStateException("Unknown outcome: " + outcome);
            }
        }
    }

    public void destroy() {
        innerDestroyFn.accept(innerContext);
    }

    public Object createSnapshot() {
        return createSnapshotFn.apply(innerContext);
    }

    public void restoreSnapshot(List<Object> objects) {
        restoreSnapshotFn.accept(innerContext, objects);
    }

}
