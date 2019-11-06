package info.jerrinot.introvertbot;

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.StreamStage;

public final class Utils {
    private Utils() {

    }

    public static FunctionEx<StreamStage<Frame>, StreamStage<Long>> countObjects(String objectClass) {
        return s -> s.map(f -> countObjectClass(f, objectClass));
    }

    private static Long countObjectClass(Frame e, String className) {
        return e.getObjects().stream().filter(o -> o.getName().equals(className)).count();
    }
}
