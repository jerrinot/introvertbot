package info.jerrinot.introvertbot.darknet;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import info.jerrinot.introvertbot.DetectedObject;
import info.jerrinot.introvertbot.Frame;

import java.util.ArrayList;
import java.util.List;

final class JsonParser {
    public enum State {
        INITIALIZED,
        ARRAY_OPENED,
        FRAME_PARSED,
        RECEIVING_OBJECTS,
        OBJECTS_RECEIVED,
    }

    private State currentState = State.INITIALIZED;

    private int frameId;
    private List<DetectedObject> detectedObjects;

    Frame feed(String item) {
        if (DarknetSource.RESET_STRING.equals(item)) {
            currentState = State.INITIALIZED;
            return null;
        }

        item = item.trim();
        switch (currentState) {
            case INITIALIZED:
                if ("[".equals(item)) {
                    currentState = State.ARRAY_OPENED;
                } else {
                    return unknownState(item);
                }
                return null;
            case ARRAY_OPENED:
                if (item.contains("frame_id")) {
                    String frameIdString = item.split(":")[1];
                    frameIdString = frameIdString.substring(0, frameIdString.length() - 1);
                    frameId = Integer.parseInt(frameIdString);
                    currentState = State.FRAME_PARSED;
                } else if ("{".equals(item)) {
                    // expected
                    // todo: should be probably extracted into its own state
                } else {
                    unknownState(item);
                }
                return null;
            case FRAME_PARSED:
                if ("\"objects\": [".equals(item)) {
                    currentState = State.RECEIVING_OBJECTS;
                } else {
                    unknownState(item);
                }
                detectedObjects = new ArrayList<>();
                return null;
            case RECEIVING_OBJECTS:
                if (item.contains("{\"class_id\":")) {
                    if (item.endsWith(",")) {
                        item = item.substring(0, item.length() - 1);
                    }
                    // todo: better JSON parsing
                    JsonObject parsedJson = Json.parse(item).asObject();
                    String name = parsedJson.getString("name", "unknown");
                    float confidence = parsedJson.getFloat("confidence", 0);
                    DetectedObject detectedObject = new DetectedObject(name, confidence);
                    detectedObjects.add(detectedObject);
                } else if ("]".equals(item)) {
                    currentState = State.OBJECTS_RECEIVED;
                } else if ("".equals(item)) {
                    // no object has been detected
                    assert detectedObjects.isEmpty();
                } else {
                    unknownState(item);
                }
                return null;
            case OBJECTS_RECEIVED:
                if ("},".equals(item)) {
                    Frame frame = new Frame(frameId, detectedObjects);
                    currentState = State.ARRAY_OPENED;
                    return frame;
                } else {
                    unknownState(item);
                }
            default:
                throw new IllegalStateException("Unexpected state: " + currentState);
        }
    }

    private Frame unknownState(String item) {
        throw new IllegalStateException("Unexpected item receive: '" + item
                + "', current state: '" + currentState + "'");
    }
}
