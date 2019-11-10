package info.jerrinot.introvertbot.sources;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class RetryableContext<INNER_CONTEXT_TYPE, ITEM_TYPE, SNAPSHOT_TYPE> implements Serializable {
    private INNER_CONTEXT_TYPE innerContext;
    private final BiConsumerEx<? super INNER_CONTEXT_TYPE, ? super SourceBuilder.TimestampedSourceBuffer<ITEM_TYPE>> innerFillBufferFn;
    private final ConsumerEx<? super INNER_CONTEXT_TYPE> innerDestroyFn;
    private final FunctionEx<? super INNER_CONTEXT_TYPE, ? extends SNAPSHOT_TYPE> createSnapshotFn;
    private final BiConsumerEx<? super INNER_CONTEXT_TYPE, ? super List<Object>> restoreSnapshotFn;
    private final Processor.Context processorContext;
    private final FunctionEx<? super Processor.Context, ? extends INNER_CONTEXT_TYPE> createFn;
    private final TriFunction<? super INNER_CONTEXT_TYPE, Throwable, Long, ErrorOutcome> errorFn;
    private final ErrorTracker errorTracker;
    private List<Object> storedSnapshot;

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
        this.errorTracker = new ErrorTracker();
    }

    public <T extends SourceBuilder.TimestampedSourceBuffer<ITEM_TYPE>> void fill(T buffer) {
        long now = System.nanoTime();
        if (errorTracker.shouldBackoff(now)) {
            return;
        }
        try {
            ensureContextExists();
            innerFillBufferFn.accept(innerContext, buffer);
            storedSnapshot = null;
            errorTracker.onSuccess();
        } catch (Throwable e) {
            errorTracker.onError(now);
            long errorDurationMillis = NANOSECONDS.toMillis(errorTracker.getErrorDurationNanos(now));
            ErrorOutcome outcome = errorFn.apply(innerContext, e, errorDurationMillis);
            switch (outcome) {
                case PROPAGATE_ERROR:
                    throw e;
                case RECREATE_CONTEXT:
                    snapshotContext();
                    innerContext = null;
                    // intentional fall-through
                case BACKOFF:
                    break;
                default:
                    throw new IllegalStateException("Unknown outcome: " + outcome);
            }
        }
    }

    private void ensureContextExists() {
        if (innerContext == null) {
            innerContext = createFn.apply(processorContext);
            if (storedSnapshot != null) {
                restoreSnapshotFn.accept(innerContext, storedSnapshot);
            }
        }
    }

    private void snapshotContext() {
        SNAPSHOT_TYPE singleSnapshotItem = null;
        if (innerContext != null) {
            singleSnapshotItem = createSnapshotFn.apply(innerContext);
        }
        if (singleSnapshotItem != null) {
            storedSnapshot = singletonList(singleSnapshotItem);
        }
    }

    public void destroy() {
        if (innerContext != null) {
            innerDestroyFn.accept(innerContext);
        }
        storedSnapshot = null;
    }

    public Object createSnapshot() {
        if (innerContext == null) {
            return storedSnapshot;
        }
        return createSnapshotFn.apply(innerContext);
    }

    public void restoreSnapshot(List<Object> objects) {
        if (innerContext == null) {
            storedSnapshot = objects;
        } else {
            restoreSnapshotFn.accept(innerContext, objects);
        }
    }

}
