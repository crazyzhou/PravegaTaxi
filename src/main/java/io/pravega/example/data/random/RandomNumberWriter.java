package io.pravega.example.data.random;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.Stream;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;

public class RandomNumberWriter {
    private final String scope;
    private final String streamName;
    private final URI controllerURI;

    public RandomNumberWriter(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run(String routingKey) {
        Random random = new Random();
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);
        try (
            ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
            EventStreamWriter<Double> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<>(),
                    EventWriterConfig.builder().build());) {
            // Double in [0, 100)
            writer.writeEvent(routingKey, random.nextDouble() * 100);
        }
    }

    public static void main(String[] args) {
        final String scope = "test-project";
        final String streamName = "taxidata";
        final URI controllerURI = URI.create("localhost:9090");
        final String routingKey = "taxidata";
        RandomNumberWriter writer = new RandomNumberWriter(scope, streamName, controllerURI);
        writer.run(routingKey);
    }
}
