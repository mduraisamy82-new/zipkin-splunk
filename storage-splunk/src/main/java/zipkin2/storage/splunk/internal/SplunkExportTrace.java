package zipkin2.storage.splunk.internal;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.splunk.Args;
import com.splunk.Index;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SplunkExportTrace extends TraceServiceGrpc.TraceServiceImplBase{

    final Index index;
    final Args indexArgs;

    private Socket socket;

    public SplunkExportTrace(Index index, Args indexArgs) {
        this.index = index;
        this.indexArgs = indexArgs;
    }

    private Socket getSocket() throws IOException {
        if(!(this.socket != null && this.socket.isConnected())){
            this.socket = index.attach(indexArgs);
        }
        return this.socket;
    }


    @Override
    public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
        try {
            OutputStream os = getSocket().getOutputStream();
            request.getResourceSpansList().forEach(span -> {
                try {
                    os.write(span.toByteArray());
                    os.write("\r\n".getBytes(UTF_8));
                    os.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.export(request, responseObserver);
    }
}
