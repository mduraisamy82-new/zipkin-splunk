package zipkin2.storage.splunk;

import com.splunk.Args;
import com.splunk.Index;
import com.splunk.Service;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SplunkOtelSpanConsumer implements SpanConsumer {

    final SplunkStorage storage;

    SplunkOtelSpanConsumer(SplunkStorage storage) {
        this.storage = storage;
    }

    @Override
    public Call<Void> accept(List<Span> spans) {
        if (spans.isEmpty()) return Call.create(null);
        return new SplunkOtelSpanConsumer.SplunkIndexCall(storage, null);
    }

    static class SplunkIndexCall extends Call.Base<Void> {
        final SplunkStorage storage;
        final Service splunk;
        final Index index;
        final Args indexArgs;
        final List<ResourceSpans> spans;

        SplunkIndexCall(SplunkStorage storage, List<ResourceSpans> spans) {
            this.storage = storage;
            this.splunk = storage.splunk();
            this.index = splunk.getIndexes().get(storage.indexName);
            this.indexArgs = storage.indexArgs;
            this.spans = spans;
        }

        @Override protected Void doExecute() throws IOException {
            try (Socket socket = index.attach(indexArgs)) {
                OutputStream os = socket.getOutputStream();
                for (ResourceSpans span : spans) {
                    os.write(span.toByteArray());
                    os.write("\r\n".getBytes(UTF_8));
                }
                os.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override protected void doEnqueue(Callback<Void> callback) {
            try (Socket socket = index.attach(indexArgs)) {
                OutputStream os = socket.getOutputStream();
                for (ResourceSpans span : spans) {
                    os.write(span.toByteArray());
                    os.write("\r\n".getBytes(UTF_8));
                }
                callback.onSuccess(null);
            } catch (Exception e) {
                e.printStackTrace();
                callback.onError(e);
            }
        }

        @Override public Call<Void> clone() {
            return new SplunkOtelSpanConsumer.SplunkIndexCall(storage, spans);
        }
    }

}
