package io.pravega.example.socket;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;

public class SocketWriter {
    private final Socket socket;
    private final String scope;
    private final String streamName;
    private final URI controllerURI;

    public SocketWriter(Socket socket, String scope, String streamName, URI controllerURI) {
        this.socket = socket;
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    // listen to a socket
    public void run(String routingKey) throws IOException {
        String message;
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);
        try (
                ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                        new JavaSerializer<>(),
                        EventWriterConfig.builder().build());
                InputStream is = socket.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            while (true) {
                message = br.readLine();
                if (message == null) continue;
                if (message.equals("FINISH")) break;
                writer.writeEvent(routingKey, message);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        final String scope = "test-project";
        final String streamName = "socket";
        final URI controllerURI = URI.create("localhost:9090");
        final String routingKey = "socket";

        // create a socket server
        final ServerSocket ss = new ServerSocket(9999);
        // wait and listen to the data input client.
        Socket socket = ss.accept();
        SocketWriter writer = new SocketWriter(socket, scope, streamName, controllerURI);

        writer.run(routingKey);

        socket.close();
        ss.close();
    }
}
