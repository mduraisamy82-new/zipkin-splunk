/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk;

import com.splunk.Args;
import com.splunk.Index;
import com.splunk.Service;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;

import static java.nio.charset.StandardCharsets.*;
import static zipkin2.storage.splunk.SplunkStorage.*;

public class SplunkSpanConsumer implements SpanConsumer {

    final SplunkStorage storage;

    SplunkSpanConsumer(SplunkStorage storage) {
        this.storage = storage;
    }

    @Override public Call<Void> accept(List<Span> spans) {
        if (spans.isEmpty()) return Call.create(null);
        return new SplunkIndexCall(storage, spans);
    }

    static class SplunkIndexCall extends Call.Base<Void> {
        final SplunkStorage storage;
        final Service splunk;
        final Index index;
        final Args indexArgs;
        final List<Span> spans;

        SplunkIndexCall(SplunkStorage storage, List<Span> spans) {
            this.storage = storage;
            this.splunk = storage.splunk();
            this.index = splunk.getIndexes().get(storage.indexName);
            this.indexArgs = storage.indexArgs;
            this.spans = spans;
        }

        @Override protected Void doExecute() throws IOException {
            System.out.println("Inside doExecute");
            try (Socket socket = index.attach(indexArgs)) {
                OutputStream os = socket.getOutputStream();
                for (Span span : spans) {
                    os.write(SpanBytesEncoder.JSON_V2.encode(span));
                    os.write("\r\n".getBytes(UTF_8));
                }
                os.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override protected void doEnqueue(Callback<Void> callback) {
            System.out.println("Inside doEnqueue");
            try (Socket socket = index.attach(indexArgs)) {
                OutputStream os = socket.getOutputStream();
                for (Span span : spans) {
                    os.write(ENCODER.encode(span));
                    os.write("\r\n".getBytes(UTF_8));
                }
                os.flush();
                callback.onSuccess(null);
            } catch (Exception e) {
                e.printStackTrace();
                callback.onError(e);
            }
        }

        @Override public Call<Void> clone() {
            return new SplunkIndexCall(storage, spans);
        }
    }
}
