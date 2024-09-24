package zipkin2.storage.splunk.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
import zipkin2.internal.JsonCodec;

import java.io.IOException;
import java.net.URISyntaxException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class OtelSpanDefaultReader implements JsonCodec.JsonReaderAdapter<Span>{

    static final Logger LOG = LoggerFactory.getLogger(OtelSpanReader.class);

    final Span.Builder builder = Span.newBuilder();


    @Override
    public Span fromJson(JsonCodec.JsonReader reader) throws IOException {

        LOG.trace("fromJson");

        long startTime =0L;
        long endTime=0L;

        this.builder.clear();
        reader.beginObject();

        while (reader.hasNext()) {

            String nextName = reader.nextName();
            if(nextName.equalsIgnoreCase("attributes")){
                processAttributes(reader);
            }else if(nextName.equalsIgnoreCase("status")){
                processStatus(reader);
            }else if(nextName.equalsIgnoreCase("trace_id")){
                builder.traceId(reader.nextString());
            }else if(nextName.equalsIgnoreCase("span_id")){
                builder.id(reader.nextString());
            }else if(nextName.equalsIgnoreCase("parentSpanId")){
                builder.parentId(reader.nextString());
            }else if(nextName.equalsIgnoreCase("name")){
                builder.name(reader.nextString());
            }else if(nextName.equalsIgnoreCase("kind")){
                Span.Kind kind = toSpanKind(reader.nextString());
                builder.kind(kind);
            }else if(nextName.equalsIgnoreCase("start_time")){
                startTime = reader.nextLong();
                builder.timestamp(toEpochMicros(startTime));
            }else if(nextName.equalsIgnoreCase("end_time")){
                 endTime =  reader.nextLong();
            }else {
                reader.skipValue();
            }
        }

        reader.endObject();
        builder.duration( Math.max(1, toEpochMicros(endTime) - toEpochMicros(startTime)));
        return builder.build();
    }

    protected void processAttributes(JsonCodec.JsonReader reader) throws IOException {
        reader.beginObject();
        while (reader.hasNext()) {
            String nextName = reader.nextName();
            builder.putTag(nextName,reader.nextString());
        }
        reader.endObject();
    }

    protected void processStatus(JsonCodec.JsonReader reader) throws IOException {
        reader.beginObject();
        while (reader.hasNext()) {
            String nextName = reader.nextName();
            builder.putTag(nextName,reader.nextString());
        }
        reader.endObject();
    }

    private static long toEpochMicros(long epochNanos) {
        return NANOSECONDS.toMicros(epochNanos);
    }

    private static Span.Kind toSpanKind(String spanType) {
        switch (spanType) {
            case "SPAN_KIND_UNSPECIFIED":
                //unspecified
                return null;
            case "SPAN_KIND_INTERNAL":
                //Internal
                return null;
            case "SPAN_KIND_SERVER":
                //Server
                return Span.Kind.SERVER;
            case "SPAN_KIND_CLIENT":
                //Client
                return Span.Kind.CLIENT;
            case "SPAN_KIND_PRODUCER":
                //Producer
                return Span.Kind.PRODUCER;
            case "SPAN_KIND_CONSUMER":
                //Consumer
                return Span.Kind.CONSUMER;
        }
        return null;
    }
}
