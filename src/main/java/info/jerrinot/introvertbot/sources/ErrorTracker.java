package info.jerrinot.introvertbot.sources;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class ErrorTracker {
    private static final long BACKOFF_NANOS = MILLISECONDS.toNanos(100);

    private long lastErrorTimestamp;
    private long firstErrorTimestamp;

    public boolean shouldBackoff(long now) {
        return lastErrorTimestamp != 0 || now < lastErrorTimestamp + BACKOFF_NANOS;
    }

    public void onRecovery() {
        lastErrorTimestamp = 0;
        firstErrorTimestamp = 0;
    }

    public void onError(long now) {
        lastErrorTimestamp = now;
        if (firstErrorTimestamp == 0) {
            firstErrorTimestamp = now;
        }
    }

    public long getErrorDurationNanos(long now) {
        return now - firstErrorTimestamp;
    }
}
