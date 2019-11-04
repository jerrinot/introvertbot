package info.jerrinot.introvertbot.source;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class DarknetSource {
    public static final String RESET_STRING = "-------------------------- TOTALLY NOT JSON --------------- RESET";

    public static StreamSource<String> fromJsonStream(String host, int port) {
        return SourceBuilder.timestampedStream("darknet-source", context -> new Context(host, port))
                .fillBufferFn(Context::fill)
                .destroyFn(Context::destroy)
                .build();
    }

    private static final class Context {
        private static final long RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1);
        private final URL url;
        private HttpURLConnection conn;
        private BufferedReader reader;
        private long errorTimestamp;
        private long retryCounter;

        public Context(String host, int port) throws IOException {
            this.url = toURL(host, port);
            initialize();
        }

        private void initialize() throws IOException {
            this.conn = createConnection(url);
            this.reader = newReader(conn);
        }

        private BufferedReader newReader(HttpURLConnection conn) throws IOException {
            BufferedReader reader;
            InputStream inputStream = conn.getInputStream();
            reader = new BufferedReader(new InputStreamReader(inputStream));
            return reader;
        }

        private HttpURLConnection createConnection(URL url) throws IOException {
            HttpURLConnection conn;
                conn = (HttpURLConnection) url.openConnection();
            return conn;
        }

        private static URL toURL(String host, int port) {
            try {
                return new URL("http://" + host + ":" + port);
            } catch (MalformedURLException e) {
                throw rethrow(e);
            }
        }


        void fill(SourceBuilder.TimestampedSourceBuffer<String> buffer) {
            try {
                if (errorTimestamp != 0) {
                    if (errorTimestamp + RETRY_DELAY_NANOS > System.nanoTime()) {
                        initialize();
                        retryCounter = 0;
                        errorTimestamp = 0;
                        buffer.add(RESET_STRING);
                    } else {
                        return;
                    }
                }
                buffer.add(reader.readLine());
            } catch (IOException e) {
                errorTimestamp = System.nanoTime();
                retryCounter++;
            }
        }

        void destroy() {
            conn.disconnect();
        }
    }
}
