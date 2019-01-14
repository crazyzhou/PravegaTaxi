/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.data;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class TaxiDataWriter {

    private final String scope;
    private final String streamName;
    private final URI controllerURI;

    public TaxiDataWriter(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run(String routingKey, Path path) {
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        try (Stream<String> stream = Files.lines(path);
             ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamWriter<TaxiRide> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<TaxiRide>(),
                     EventWriterConfig.builder().build());
             ) {
            stream.forEach(line -> {
                writer.writeEvent(routingKey, TaxiRide.fromString(line));
                System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                        line, routingKey, scope, streamName);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final String scope = "test-project";
        final String streamName = "taxidata";
        final URI controllerURI = URI.create("localhost:9090");
        final Path path = Paths.get("test.csv");
        TaxiDataWriter writer = new TaxiDataWriter(scope, streamName, controllerURI);

        final String routingKey = "taxidata";
        writer.run(routingKey, path);
    }
}