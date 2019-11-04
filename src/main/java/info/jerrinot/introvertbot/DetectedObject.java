package info.jerrinot.introvertbot;

import java.io.Serializable;

public final class DetectedObject implements Serializable {
    private final String name;
    private final double confidence;

    public DetectedObject(String name, double confidence) {
        this.name = name;
        this.confidence = confidence;
    }

    public double getConfidence() {
        return confidence;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DetectedObject{" +
                "name='" + name + '\'' +
                ", confidence=" + confidence +
                '}';
    }
}
