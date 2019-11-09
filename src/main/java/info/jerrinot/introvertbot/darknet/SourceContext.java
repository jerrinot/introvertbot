package info.jerrinot.introvertbot.darknet;

import com.hazelcast.jet.pipeline.SourceBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

final class SourceContext {
    private final URL url;
    private BufferedReader reader;

    public SourceContext(String host, int port) throws MalformedURLException {
        this.url = new URL("http://" + host + ":" + port);
    }

    private BufferedReader connect() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        InputStream inputStream = conn.getInputStream();
        return new BufferedReader(new InputStreamReader(inputStream));
    }

    void fill(SourceBuilder.TimestampedSourceBuffer<String> buffer) throws IOException {
        if (reader == null) {
            reader = connect();
            buffer.add(DarknetSource.HELLO_MESSAGE);
        }
        buffer.add(reader.readLine());
    }

    void destroy() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
