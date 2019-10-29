package info.jerrinot.introvertbot;

public final class JsonParser {
    public enum State {
        INITIALIZED,
        ARRAY_OPENED,
        FRAME_PARSED,
        RECEIVING_OBJECTS,
        OBJECT_RECEIVED,
        FRAME_FINISHED
    }

    private State currentState = State.INITIALIZED;

    public Frame feed(String item) {
        item = item.trim();
        switch (currentState) {
            case INITIALIZED:
                if ("[".equals(item)) {
                    currentState = State.ARRAY_OPENED;
                } else {
                    throw new IllegalStateException("Unexpected item receive: '" + item
                            + "', current state: '" + currentState + "'");
                }
                return null;
            case ARRAY_OPENED:

                break;
            case FRAME_PARSED:
                break;
            case RECEIVING_OBJECTS:
                break;
            case OBJECT_RECEIVED:
                break;
            case FRAME_FINISHED:
                break;
            default:
                throw new IllegalStateException("Unexpected state: " + currentState);
        }
    }
}
