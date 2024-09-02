package zipkin2.storage.splunk;

import com.google.protobuf.util.JsonFormat;
import com.splunk.Args;
import com.splunk.Index;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SplunkCollectorMetrics extends MetricsServiceGrpc.MetricsServiceImplBase{

    static final Logger LOG = LoggerFactory.getLogger(SplunkCollectorMetrics.class);

    private final SplunkCollectorMetrics.SplunkIndexCall splunkIndexCall;

    public SplunkCollectorMetrics(Index index, Args indexArgs) {
        this.splunkIndexCall = new SplunkCollectorMetrics.SplunkIndexCall(index, indexArgs);
    }

    @Override
    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        try {
            this.splunkIndexCall.doExecute(request.getResourceMetricsList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ExportMetricsPartialSuccess exportMetricsPartialSuccess = ExportMetricsPartialSuccess.newBuilder().setRejectedDataPoints(0).buildPartial();
        ExportMetricsServiceResponse exportMetricsServiceResponse = ExportMetricsServiceResponse.newBuilder().setPartialSuccess(exportMetricsPartialSuccess).build();
        responseObserver.onNext(exportMetricsServiceResponse);
        responseObserver.onCompleted();
    }

    static class SplunkIndexCall {
        final Index index;
        final Args indexArgs;

        SplunkIndexCall(Index index , Args indexArgs) {
            this.index = index;
            this.indexArgs = indexArgs;
        }

        protected Void doExecute(List<ResourceMetrics> resourceMetrics) throws IOException {

                for (ResourceMetrics resourceMetric : resourceMetrics) {
                    for(ScopeMetrics scopeMetrics : resourceMetric.getScopeMetricsList()){
                        for(Metric metric : scopeMetrics.getMetricsList()){
                            ResourceMetrics rm = ResourceMetrics.newBuilder().
                                    setResource(resourceMetric.getResource()).
                                    addScopeMetrics(ScopeMetrics.newBuilder().
                                            setScope(scopeMetrics.getScope()).
                                            addMetrics(metric).build())
                                    .build() ;
                            LOG.debug("RM: {} ",JsonFormat.printer().omittingInsignificantWhitespace().print(rm));
                        try (Socket socket = index.attach(indexArgs)) {
                            OutputStream os = socket.getOutputStream();
                            os.write(JsonFormat.printer().omittingInsignificantWhitespace().print(rm).getBytes(UTF_8));
                            os.write("\r\n".getBytes(UTF_8));
                            os.flush();
                        }
                         catch (Exception e) {
                             System.out.println(e.getMessage());
                             e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            return null;
        }

    }

}
