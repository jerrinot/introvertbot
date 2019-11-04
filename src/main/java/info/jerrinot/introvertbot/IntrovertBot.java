package info.jerrinot.introvertbot;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import info.jerrinot.introvertbot.source.DarknetSource;

import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class IntrovertBot {
    private static final String HOST = "10.0.0.61";
    private static final int PORT = 8090;

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(DarknetSource.fromJsonStream(HOST, PORT))
                .withNativeTimestamps(0)
                .mapStateful(JsonParser::new, JsonParser::feed)
                .map(Utils::countPersons)
                .window(sliding(10_000, 1_000))
                .aggregate(averagingLong(e -> e))
                .map(WindowResult::result)
                .map(Math::round)
                .drainTo(logger());

        JetInstance jet = Jet.newJetInstance();
        jet.newJob(pipeline).join();
    }
}
