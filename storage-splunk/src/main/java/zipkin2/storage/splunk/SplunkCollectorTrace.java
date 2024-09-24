package zipkin2.storage.splunk;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.internal.shaded.bouncycastle.util.encoders.Hex;
import com.splunk.Args;
import com.splunk.Index;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SplunkCollectorTrace extends TraceServiceGrpc.TraceServiceImplBase{

    static final Logger LOG = LoggerFactory.getLogger(SplunkCollectorTrace.class);

    private final SplunkIndexCall splunkIndexCall;

    public SplunkCollectorTrace(Index index, Args indexArgs) {
        this.splunkIndexCall = new SplunkIndexCall(index, indexArgs);
    }
    //To do
    @Override
    public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
        try {
            this.splunkIndexCall.doExecute(request.getResourceSpansList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ExportTracePartialSuccess exportTracePartialSuccess = ExportTracePartialSuccess.newBuilder().setRejectedSpans(0).buildPartial();
        ExportTraceServiceResponse exportTraceServiceResponse = ExportTraceServiceResponse.newBuilder().setPartialSuccess(exportTracePartialSuccess).build();
        responseObserver.onNext(exportTraceServiceResponse);
        responseObserver.onCompleted();
    }


    static class SplunkIndexCall {
        final Index index;
        final Args indexArgs;

        SplunkIndexCall(Index index , Args indexArgs) {
            this.index = index;
            this.indexArgs = indexArgs;
        }

        protected Void doExecute(List<ResourceSpans> spans) throws IOException {
            try (Socket socket = index.attach(indexArgs)) {
                OutputStream os = socket.getOutputStream();
                for (ResourceSpans span : spans) {
                    LOG.debug("SpanID: {}",  span.getScopeSpansList().get(0).getSpans(0).getSpanId().toStringUtf8());
                    LOG.debug("SpanID: {} Base64",  BaseEncoding.base64().encode((span.getScopeSpansList().get(0).getSpans(0).getSpanId()).toByteArray()));
                    LOG.debug("SpanID: {} HEX", Hex.toHexString(span.getScopeSpansList().get(0).getSpans(0).getSpanId().toByteArray()));
                    LOG.debug("MessageOrBuilder: {}", JsonFormat.printer().omittingInsignificantWhitespace().print(span.getResource()));
                    os.write(JsonFormat.printer().omittingInsignificantWhitespace().print(span).getBytes(UTF_8));
                    os.write("\r\n".getBytes(UTF_8));
                }
                os.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

    }
}
