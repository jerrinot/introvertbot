package info.jerrinot.introvertbot;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import info.jerrinot.introvertbot.darknet.DarknetSource;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.contrib.influxdb.InfluxDbSinks.influxDb;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static info.jerrinot.introvertbot.Utils.countObjects;
import static info.jerrinot.introvertbot.darknet.DarknetSource.json2Frame;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.influxdb.BatchOptions.DEFAULTS;
import static org.influxdb.InfluxDB.LogLevel.FULL;

public class IntrovertBot {
//    private static final String HOST = "10.0.0.61";
    private static final String DARKNET_HOST = "192.168.1.111";
    private static final int DARKNET_PORT = 8090;
    private static final String INFLUXDB_PASSWORD_PROPNAME = "introvert.influxdb.password";

    private static final String INFLUXDB_URL = "https://corlysis.com:8086";
    private static final String INFLUXDB_USERNAME = "token";
    private static final String INFLUXDB_PASSWORD = System.getProperty(INFLUXDB_PASSWORD_PROPNAME);
    private static final String INFLUXDB_DATABASE = "introvertbot";
    private static final String INFLUXDB_RETENTION_POLICY = "trial";
    private static final int INFLUXDB_TIMEOUT_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        if (INFLUXDB_PASSWORD == null || INFLUXDB_PASSWORD.isEmpty()) {
            throw new IllegalArgumentException("Property '" + INFLUXDB_PASSWORD_PROPNAME +"' is not set to InfluxDB password");
        }

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(DarknetSource.readJsonStream(DARKNET_HOST, DARKNET_PORT))
                .withNativeTimestamps(0)
                .apply(json2Frame())
                .apply(countObjects("person"))
                .window(sliding(10_000, 1_000))
                .aggregate(maxBy(Long::compareTo))
                .map(WindowResult::result)
                .map(e -> Point.measurement("people")
                        .addField("value", e)
                        .time(System.currentTimeMillis(), MILLISECONDS)
                        .tag("tag", "count")
                        .build())
                .drainTo(influxDb("influx-sink", () -> {
                            OkHttpClient.Builder clientBuilder = new OkHttpClient().newBuilder()
                                    .connectTimeout(INFLUXDB_TIMEOUT_SECONDS, SECONDS)
                                    .readTimeout(INFLUXDB_TIMEOUT_SECONDS, SECONDS)
                                    .writeTimeout(INFLUXDB_TIMEOUT_SECONDS, SECONDS);
                            return InfluxDBFactory.connect(INFLUXDB_URL, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, clientBuilder)
                                    .setDatabase(INFLUXDB_DATABASE)
                                    .setLogLevel(FULL)
                                    .enableBatch(DEFAULTS.exceptionHandler(
                                            (points, throwable) -> rethrow(throwable)))
                                    .setRetentionPolicy(INFLUXDB_RETENTION_POLICY);
                        }));

        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        JetInstance jet1 = Jet.newJetInstance(jetConfig);
        Job job = jet1.newJob(pipeline);
        job.join();
    }
}
