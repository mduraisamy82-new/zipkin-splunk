/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.JsonCodec;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class OtelSpanReader implements JsonCodec.JsonReaderAdapter<Span> {

    static final Logger LOG = LoggerFactory.getLogger(OtelSpanReader.class);

    final Span.Builder builder = Span.newBuilder();

    @Override
    public Span fromJson(JsonCodec.JsonReader reader) throws IOException {
        LOG.trace("fromJson");

        this.builder.clear();
        reader.beginObject();

        while (reader.hasNext()) {
            String nextName = reader.nextName();
            if(nextName.equalsIgnoreCase("resource")){
                processResource(reader);
            }
            else if(nextName.equalsIgnoreCase("scopeSpans")){
                try {
                    processScopeSpans(reader);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }else {
                reader.skipValue();
            }
        }
        reader.endObject();
        return builder.build();
    }


    protected void processResource(JsonCodec.JsonReader reader) throws IOException {
            String nextName;
            reader.beginObject();
            while (reader.hasNext()) {
                nextName = reader.nextName();
                if(nextName.equalsIgnoreCase("attributes")){
                    processResourceAttributes(reader);
                }else{
                    reader.skipValue();
                }

            }
            reader.endObject();
    }

    protected void processResourceAttributes(JsonCodec.JsonReader reader) throws IOException {
        String nextName;
        reader.beginArray();
        String keyValue;
        while (reader.hasNext()) {
            reader.beginObject();
            while (reader.hasNext()) {
                nextName = reader.nextName();
                if (nextName.equalsIgnoreCase("key")) {
                    keyValue = reader.nextString();
                    if (keyValue.equalsIgnoreCase("service.name")) {
                        // Move to value
                        reader.nextName();
                        // Assuming it is always "value"
                        reader.beginObject();
                        // StringValue
                        reader.nextName();
                        String serviceName = reader.nextString();
                        builder.localEndpoint(getEndpoint(serviceName,"",0));
                        LOG.trace("Value {}", serviceName);
                        reader.endObject();
                    }
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
        }
        reader.endArray();

    }

    protected void processScopeSpans(JsonCodec.JsonReader reader) throws IOException, URISyntaxException {
        String nextName;
        Span.Kind kind = null;
        reader.beginArray();
        while (reader.hasNext()) {
            reader.beginObject();
            while (reader.hasNext()){
                nextName = reader.nextName();
                if(nextName.equalsIgnoreCase("spans")){
                    reader.beginArray();
                    while (reader.hasNext()){
                        reader.beginObject();
                        long startTime =0L;
                        while (reader.hasNext()){
                            nextName = reader.nextName();
                            if(nextName.equalsIgnoreCase("spanId")){
                                builder.id(reader.nextString());
                            }else if(nextName.equalsIgnoreCase("traceId")){
                                builder.traceId(reader.nextString());
                            }
                            else if(nextName.equalsIgnoreCase("parentSpanId")){
                                builder.parentId(reader.nextString());
                            }
                            else if(nextName.equalsIgnoreCase("kind")){
                                kind = toSpanKind(reader.nextInt());
                                builder.kind(kind);
                            }else if(nextName.equalsIgnoreCase("name")){
                                builder.name(reader.nextString());
                            }
                            else if(nextName.equalsIgnoreCase("startTimeUnixNano")){
                                startTime = reader.nextLong();
                                builder.timestamp(toEpochMicros(startTime));
                            }
                            else if(nextName.equalsIgnoreCase("endTimeUnixNano")){
                                long endTime =  reader.nextLong();
                                builder.duration( Math.max(1, toEpochMicros(endTime) - toEpochMicros(startTime)));
                            }
                            else if(nextName.equalsIgnoreCase("attributes")){
                                processSpanAttributes(reader , kind);
                            }
                            else {
                                reader.skipValue();
                            }
                        }
                        reader.endObject();
                    }
                    reader.endArray();
                }else {
                    reader.skipValue();
                }
            }
            reader.endObject();
        }
        reader.endArray();
    }

    protected void processSpanAttributes(JsonCodec.JsonReader reader , Span.Kind kind) throws IOException, URISyntaxException {
        String nextName;
        int latestIntValue;
        String latestStringValue;

        // remote Endpoint
        String remoteServiceName = "Unknown";
        String remoteIP = "N/A";
        int remotePort = 0;

        reader.beginArray();
        while (reader.hasNext()) {
            reader.beginObject();

            //Reset
            latestIntValue = 0;
            latestStringValue= "";

            String key = "";
            nextName = reader.nextName();
            if(nextName.equalsIgnoreCase("key")){
                key = reader.nextString();
            }
            nextName = reader.nextName();
            if(nextName.equalsIgnoreCase("value")) {
                reader.beginObject();
                String type = reader.nextName();
                if(type.equalsIgnoreCase("intValue")){
                    latestIntValue = reader.nextInt();
                    builder.putTag(key, String.valueOf(latestIntValue));
                } else if (type.equalsIgnoreCase("booleanValue")) {
                    builder.putTag(key, String.valueOf(reader.nextBoolean()));
                } else if (type.equalsIgnoreCase("stringValue")) {
                    latestStringValue = reader.nextString();
                    builder.putTag(key, latestStringValue);
                }else{
                    reader.skipValue();
                }

                LOG.trace("key {}, latestIntValue {}, latestStringValue {}",key, latestIntValue, latestStringValue);

                // Remote EndPoint
                if(key.equalsIgnoreCase("server.port") ||
                        key.equalsIgnoreCase("network.peer.port") ||
                        key.equalsIgnoreCase("server.socket.port") ||
                        key.equalsIgnoreCase("net.sock.peer.port")
                  ){
                    remotePort = latestIntValue;
                }else if(key.equalsIgnoreCase("server.address") ||
                        key.equalsIgnoreCase("net.peer.name") ||
                        key.equalsIgnoreCase("network.peer.address") ||
                        key.equalsIgnoreCase("server.socket.domain") ||
                        key.equalsIgnoreCase("server.socket.address") ||
                        key.equalsIgnoreCase("net.sock.peer.name") ||
                        key.equalsIgnoreCase("net.sock.peer.addr") ||
                        key.equalsIgnoreCase("peer.hostname") ||
                        key.equalsIgnoreCase("peer.address")
                        ){
                    remoteIP = latestStringValue;
                }else if(key.equalsIgnoreCase("db.name") ||
                        key.equalsIgnoreCase("peer.service") ||
                        key.equalsIgnoreCase("url.full") ||
                        key.equalsIgnoreCase("messaging.destination.name")){
                    if( key.equalsIgnoreCase("url.full")){
                        URI uri = new URI(latestStringValue);
                        String path = uri.getPath();
                        String[] segments = path.split("/");
                        remoteServiceName = segments[segments.length - 1];
                    }else {
                        remoteServiceName = latestStringValue;
                    }
                }

                reader.endObject();
            }
            reader.endObject();
        }
        reader.endArray();
        // process for only type 3 & 4
        if(kind !=null &&
                (kind.equals(Span.Kind.CLIENT) ||  kind.equals(Span.Kind.PRODUCER))) {
            builder.remoteEndpoint(getEndpoint(remoteServiceName, remoteIP, remotePort));
        }
    }


    protected Endpoint getEndpoint(String serviceNameValue, String host, int port) {
        LOG.trace("serviceNameValue {}, host {}, port {}",serviceNameValue, host, port);
        Endpoint.Builder endpoint = Endpoint.newBuilder();
        endpoint.ip(host);
        endpoint.serviceName(serviceNameValue);
        endpoint.port(port);
        return endpoint.build();
    }

    private static long toEpochMicros(long epochNanos) {
        return NANOSECONDS.toMicros(epochNanos);
    }

    // Ref https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto

    private static Span.Kind toSpanKind(int spanType) {
        switch (spanType) {
            case 0:
                //Unspecified
                return null;
            case 1:
                //Internal
                return null;
            case 2:
                //Server
                return Span.Kind.SERVER;
            case 3:
                //Client
                return Span.Kind.CLIENT;
            case 4:
                //Producer
                return Span.Kind.PRODUCER;
            case 5:
                //Consumer
                return Span.Kind.CONSUMER;
        }
        return null;
    }

   /* private interface NextNameProcessor{
        void processElement(String elementName,JsonCodec.JsonReader reader) throws IOException;
    }

    private class spanAttributesProcessor implements NextNameProcessor {
        @Override
        public void processElement(String elementName, JsonCodec.JsonReader reader) throws IOException {
            builder.clearTags();
            reader.beginArray();
            while (reader.hasNext()) {
                reader.beginObject();
                while (reader.hasNext()) {
                    String key = reader.nextName();
                    if (!reader.peekNull()) {
                        if (reader.peekString()) {
                            builder.putTag(key, reader.nextString());
                        } else if (reader.peekBoolean()) {
                            builder.putTag(key, String.valueOf(reader.nextBoolean()));
                        } else
                            builder.putTag(key, String.valueOf(reader.nextInt()));
                    }
                }
                reader.endObject();
            }
            reader.endArray();
        }
    }

    private class defaultSkipNextNameProcessor implements NextNameProcessor{

        @Override
        public void processElement(String elementName, JsonCodec.JsonReader reader) throws IOException{
            reader.skipValue();
        }
    }

    private class NextNameProcessorFactory{

        NextNameProcessor defaultSkipNextNameProcessor = new defaultSkipNextNameProcessor();

        NextNameProcessor getNextNameProcessor(String elementName){
            return switch (elementName) {
                default -> defaultSkipNextNameProcessor;
            };
        }
    }
    */
}