package info.jerrinot.introvertbot;

import java.io.Serializable;
import java.util.List;

public final class Frame implements Serializable {
    private final int id;
    private final List<DetectedObject> objects;

    public Frame(int id, List<DetectedObject> objects) {
        this.id = id;
        this.objects = objects;
    }

    public List<DetectedObject> getObjects() {
        return objects;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Frame{" +
                "id=" + id +
                ", objects=" + objects +
                '}';
    }
}
