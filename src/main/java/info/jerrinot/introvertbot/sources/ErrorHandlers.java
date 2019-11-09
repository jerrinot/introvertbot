package info.jerrinot.introvertbot.sources;

import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.TriFunction;

import java.util.concurrent.TimeUnit;

public final class ErrorHandlers {
    private static final TriFunction<?, Throwable, Long, ErrorOutcome> ALWAYS_PROPAGATE = (c, t, l) -> ErrorOutcome.PROPAGATE_ERROR;

    private ErrorHandlers() {

    }

    public static TriFunction<?, Throwable, Long, ErrorOutcome> alwaysPropagate() {
        return ALWAYS_PROPAGATE;
    }

    public static TriFunction<?, Throwable, Long, ErrorOutcome> timeout(ErrorOutcome outcome, long timeout, TimeUnit timeUnit) {
        long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
        return (c, t, l) -> System.nanoTime() < deadline ? outcome : ErrorOutcome.PROPAGATE_ERROR;
    }

    public static <T> TriFunction<T, Throwable, Long, ErrorOutcome> timeoutAndFilter(ErrorOutcome outcome,
                                                                                     long timeout, TimeUnit timeUnit, PredicateEx<Throwable> predicate) {
        long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
        return (c, t, l) -> {
            if (predicate.test(t) && System.nanoTime() < deadline) {
                return outcome;
            }
            return ErrorOutcome.PROPAGATE_ERROR;
        };
    }

    public static PredicateEx<Throwable> allow(Class<? extends Throwable> allowedType) {
        return allowedType::isInstance;
    }
}
