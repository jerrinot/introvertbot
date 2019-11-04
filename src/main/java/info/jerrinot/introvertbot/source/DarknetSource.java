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

import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class DarknetSource {
    public static StreamSource<String> fromJsonStream(String host, int port) {
        return SourceBuilder.timestampedStream("darknet-source", context -> new Context(host, port))
                .fillBufferFn(Context::fill)
                .destroyFn(Context::destroy)
                .build();
    }

    private static final class Context {
        private final HttpURLConnection conn;
        private final BufferedReader reader;

        public Context(String host, int port) {
            URL url = toURL(host, port);

            this.conn = createConnection(url);
            this.reader = newReader(conn);
        }

        private BufferedReader newReader(HttpURLConnection conn) {
            BufferedReader reader;
            try {
                InputStream inputStream = conn.getInputStream();
                reader = new BufferedReader(new InputStreamReader(inputStream));
            } catch (IOException e) {
                throw rethrow(e);
            }
            return reader;
        }

        private HttpURLConnection createConnection(URL url) {
            HttpURLConnection conn;
            try {
                conn = (HttpURLConnection) url.openConnection();
            } catch (IOException e) {
                throw rethrow(e);
            }
            return conn;
        }

        private static URL toURL(String host, int port) {
            try {
                return new URL("http://" + host + ":" + port);
            } catch (MalformedURLException e) {
                throw rethrow(e);
            }
        }


        void fill(SourceBuilder.TimestampedSourceBuffer<String> buffer) throws IOException {
            buffer.add(reader.readLine());
        }

        void destroy() {
            conn.disconnect();
        }
    }
}
