package zipkin2.storage.splunk;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.splunk.Args;
import com.splunk.Index;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
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

        protected Void doExecute(List<? extends MessageOrBuilder> spans) throws IOException {
            try (Socket socket = index.attach(indexArgs)) {
                OutputStream os = socket.getOutputStream();
                for (MessageOrBuilder span : spans) {
                    LOG.debug("MessageOrBuilder: {}", JsonFormat.printer().omittingInsignificantWhitespace().print(span));
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
