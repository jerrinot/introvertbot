package info.jerrinot.introvertbot.darknet;

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import info.jerrinot.introvertbot.Frame;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public final class DarknetSource {
    public static final String RESET_STRING = "-------------------------- TOTALLY NOT JSON --------------- RESET";

    public static StreamSource<String> readJsonStream(String host, int port) {
        return SourceBuilder.timestampedStream("darknet-source", context -> new Context(host, port))
                .fillBufferFn(Context::fill)
                .destroyFn(Context::destroy)
                .build();
    }


    public static FunctionEx<StreamStage<String>, StreamStage<Frame>> json2Frame() {
        return e -> e.mapStateful(JsonParser::new, JsonParser::feed);
    }

    private static final class Context {
        private static final long RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1);
        private final URL url;
        private BufferedReader reader;
        private long errorTimestamp;

        public Context(String host, int port) throws MalformedURLException {
            this.url = new URL("http://" + host + ":" + port);
        }

        private BufferedReader reconnect() throws IOException {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            InputStream inputStream = conn.getInputStream();
            return new BufferedReader(new InputStreamReader(inputStream));
        }

        void fill(SourceBuilder.TimestampedSourceBuffer<String> buffer) {
            try {
                if (reader == null) {
                    if (errorTimestamp == 0 || errorTimestamp + RETRY_DELAY_NANOS > System.nanoTime()) {
                        reader = reconnect();
                    } else {
                        return;
                    }
                    buffer.add(RESET_STRING);
                }

                buffer.add(reader.readLine());
            } catch (IOException e) {
                errorTimestamp = System.nanoTime();
            }
        }

        void destroy() throws IOException {
            reader.close();
        }
    }
}
