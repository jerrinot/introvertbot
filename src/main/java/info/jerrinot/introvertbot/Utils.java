package info.jerrinot.introvertbot;

import com.hazelcast.jet.function.FunctionEx;

public final class Utils {
    private Utils() {

    }

    public static Long countPersons(Frame e) {
        return countObjectClass(e, "person");
    }

    public static Long countObjectClass(Frame e, String className) {
        return e.getObjects().stream().filter(o -> o.getName().equals(className)).count();
    }
}
