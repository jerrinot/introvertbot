package info.jerrinot.introvertbot;

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.StreamStage;
import org.influxdb.dto.Point;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class Utils {
    private Utils() {

    }

    public static FunctionEx<StreamStage<Long>, StreamStage<Point>> toInfluxDBPoint() {
        return s -> s.map(e -> Point.measurement("people")
                .addField("value", e)
                .time(System.currentTimeMillis(), MILLISECONDS)
                .tag("tag", "count")
                .build());
    }

    public static FunctionEx<StreamStage<Frame>, StreamStage<Long>> countObjects(String objectClass) {
        return s -> s.map(f -> countObjectClass(f, objectClass));
    }

    private static Long countObjectClass(Frame e, String className) {
        return e.getObjects().stream().filter(o -> o.getName().equals(className)).count();
    }
}
