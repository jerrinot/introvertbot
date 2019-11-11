package info.jerrinot.introvertbot.retryablesources.internal;

import static java.lang.Math.min;

public final class ErrorTracker {
    private static final long MAX_SHIFT = 12;

    private long lastErrorTimestamp;
    private long firstErrorTimestamp;
    private long errorCounter;


    public boolean shouldBackoff(long now) {
        if (errorCounter == 0) {
            return false;
        }
        int backoffNanos = 1 << (min(errorCounter, MAX_SHIFT));
        return now < lastErrorTimestamp + backoffNanos;
    }

    public void onSuccess() {
        lastErrorTimestamp = 0;
        firstErrorTimestamp = 0;
        errorCounter = 0;
    }

    public void onError(long now) {
        lastErrorTimestamp = now;
        if (firstErrorTimestamp == 0) {
            firstErrorTimestamp = now;
        }
        errorCounter++;
    }

    public long getErrorDurationNanos(long now) {
        return now - firstErrorTimestamp;
    }
}