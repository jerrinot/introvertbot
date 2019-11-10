package info.jerrinot.introvertbot.sources;

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.function.PredicateEx.alwaysTrue;
import static info.jerrinot.introvertbot.sources.ErrorOutcome.PROPAGATE_ERROR;

public final class ErrorHandlers {
    private static final TriFunction<?, Throwable, Long, ErrorOutcome> ALWAYS_PROPAGATE = (c, t, l) -> PROPAGATE_ERROR;

    private ErrorHandlers() {

    }

    public static TriFunction<?, Throwable, Long, ErrorOutcome> alwaysPropagate() {
        return ALWAYS_PROPAGATE;
    }

    public static TriFunction<?, Throwable, Long, ErrorOutcome> fixedTimeout(ErrorOutcome outcome, long timeout, TimeUnit timeUnit) {
        return fixedTimeoutAndFilter(outcome, timeout, timeUnit, alwaysTrue());
    }

    public static <T> TriFunction<T, Throwable, Long, ErrorOutcome> fixedTimeoutAndFilter(ErrorOutcome outcome,
                                                                                          long timeout, TimeUnit timeUnit, PredicateEx<Throwable> predicate) {
        return (c, t, l) -> {
            if (predicate.test(t) && l < timeUnit.toMillis(timeout)) {
                return outcome;
            }
            return PROPAGATE_ERROR;
        };
    }

    public static <T> TriFunction<T, Throwable, Long, ErrorOutcome> configuredTimeout(ErrorOutcome outcome, FunctionEx<T, Long> foo) {
        return (c, t, l) -> {
            Long timeout = foo.apply(c);
            return l < timeout ? outcome : PROPAGATE_ERROR;
        };
    }

    public static PredicateEx<Throwable> allowOnly(Class<? extends Throwable> allowedType) {
        return allowedType::isInstance;
    }
}
