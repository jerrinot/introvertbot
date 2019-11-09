package info.jerrinot.introvertbot.darknet;

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import info.jerrinot.introvertbot.Frame;
import info.jerrinot.introvertbot.sources.RetryableSourceBuilder;

import java.io.IOException;

import static info.jerrinot.introvertbot.sources.ErrorHandlers.allowOnly;
import static info.jerrinot.introvertbot.sources.ErrorHandlers.timeoutAndFilter;
import static info.jerrinot.introvertbot.sources.ErrorOutcome.RECREATE_CONTEXT;
import static java.util.concurrent.TimeUnit.MINUTES;

public final class DarknetSource {
    public static final String HELLO_MESSAGE = "-------------------------- TOTALLY NOT JSON --------------- EHLO";

    public static StreamSource<String> readJsonStream(String host, int port) {
        String srcName = "darknet-source-" + host + ":" + port;
        return RetryableSourceBuilder.timestampedStream(srcName, context -> new SourceContext(host, port))
                .fillBufferFn(SourceContext::fill)
                .destroyFn(SourceContext::destroy)
                .errorFn(timeoutAndFilter(RECREATE_CONTEXT, 1, MINUTES, allowOnly(IOException.class)))
                .build();
    }

    public static FunctionEx<StreamStage<String>, StreamStage<Frame>> json2Frame() {
        return e -> e.mapStateful(JsonParser::new, JsonParser::feed);
    }
}
