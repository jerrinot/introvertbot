package info.jerrinot.introvertbot.sources;

/**
 * Result of error handling function.
 *
 */
public enum ErrorOutcome {

    /**
     * Immediately propagate error to Jet. This will likely result in a job failure.
     *
     */
    PROPAGATE_ERROR,

    /**
     * Backoff and try again later.
     *
     */
    BACKOFF,

    /**
     * Create a new context before trying again.
     * Jet may apply backoff.
     *
     */
    RECREATE_CONTEXT
}
