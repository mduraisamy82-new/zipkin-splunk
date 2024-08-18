/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk.internal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.*;
import com.splunk.Service;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesEncoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.splunk.SplunkStorage;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.*;

import static com.linecorp.armeria.common.HttpHeaderNames.CACHE_CONTROL;
import static com.linecorp.armeria.common.HttpStatus.BAD_REQUEST;
import static com.linecorp.armeria.common.HttpStatus.NOT_FOUND;
import static com.linecorp.armeria.common.MediaType.ANY_TEXT_TYPE;

public class ZipkinSplunkQueryApiV2{
    final String storageType;
    final SplunkStorage storage; // don't cache spanStore here as it can cause the app to crash!
    final long defaultLookback;
    /**
     * The Cache-Control max-age (seconds) for /api/v2/services /api/v2/remoteServices and
     * /api/v2/spans
     */
    final int namesMaxAge;
    final List<String> autocompleteKeys;

    volatile int serviceCount; // used as a threshold to start returning cache-control headers

    public ZipkinSplunkQueryApiV2(
            StorageComponent storage,
            String storageType,
            long defaultLookback, // 1 day in millis
            int namesMaxAge, // 5 minutes
            List<String> autocompleteKeys
    ) {
        this.storage = (SplunkStorage)storage;
        this.storageType = storageType;
        this.defaultLookback = defaultLookback;
        this.namesMaxAge = namesMaxAge;
        this.autocompleteKeys = autocompleteKeys;
    }

    @Post("/api/v2/login")
    @Blocking
    public HttpResponse login(
            LoginForm loginForm) throws IOException {
        Service svc = storage.login(loginForm);
        return HttpResponse.ofJson(new Token(svc.getToken()));
    }

    @Get("/api/v2/dependencies")
    @Blocking
    public AggregatedHttpResponse getDependencies(
            @Param("endTs") long endTs,
            @Param("lookback") Optional<Long> lookback) throws IOException {
        Call<List<DependencyLink>> call =
                storage.spanStore().getDependencies(endTs, lookback.orElse(defaultLookback));
        return jsonResponse(DependencyLinkBytesEncoder.JSON_V1.encodeList(call.execute()));
    }

    @Get("/api/v2/services")
    @Blocking
    public AggregatedHttpResponse getServiceNames(ServiceRequestContext ctx) throws IOException {
        List<String> serviceNames = storage.serviceAndSpanNames().getServiceNames().execute();
        serviceCount = serviceNames.size();
        return maybeCacheNames(serviceCount > 3, serviceNames, ctx.alloc());
    }

    @Get("/api/v2/spans")
    @Blocking
    public AggregatedHttpResponse getSpanNames(
            @Param("serviceName") String serviceName, ServiceRequestContext ctx)
            throws IOException {
        List<String> spanNames = storage.serviceAndSpanNames().getSpanNames(serviceName).execute();
        return maybeCacheNames(serviceCount > 3, spanNames, ctx.alloc());
    }

    @Get("/api/v2/remoteServices")
    @Blocking
    public AggregatedHttpResponse getRemoteServiceNames(
            @Param("serviceName") String serviceName, ServiceRequestContext ctx)
            throws IOException {
        List<String> remoteServiceNames =
                storage.serviceAndSpanNames().getRemoteServiceNames(serviceName).execute();
        return maybeCacheNames(serviceCount > 3, remoteServiceNames, ctx.alloc());
    }

    @Get("/api/v2/traces")
    @Blocking
    public AggregatedHttpResponse getTraces(
            @Param("serviceName") Optional<String> serviceName,
            @Param("remoteServiceName") Optional<String> remoteServiceName,
            @Param("spanName") Optional<String> spanName,
            @Param("annotationQuery") Optional<String> annotationQuery,
            @Param("minDuration") Optional<Long> minDuration,
            @Param("maxDuration") Optional<Long> maxDuration,
            @Param("endTs") Optional<Long> endTs,
            @Param("lookback") Optional<Long> lookback,
            @Default("10") @Param("limit") int limit)
            throws IOException {
        QueryRequest queryRequest =
                QueryRequest.newBuilder()
                        .serviceName(serviceName.orElse(null))
                        .remoteServiceName(remoteServiceName.orElse(null))
                        .spanName(spanName.orElse(null))
                        .parseAnnotationQuery(annotationQuery.orElse(null))
                        .minDuration(minDuration.orElse(null))
                        .maxDuration(maxDuration.orElse(null))
                        .endTs(endTs.orElse(System.currentTimeMillis()))
                        .lookback(lookback.orElse(defaultLookback))
                        .limit(limit)
                        .build();

        List<List<Span>> traces = storage.spanStore().getTraces(queryRequest).execute();
        return jsonResponse(writeTraces(SpanBytesEncoder.JSON_V2, traces));
    }

    @Get("/api/v2/trace/{traceId}")
    @Blocking
    public AggregatedHttpResponse getTrace(@Param("traceId") String traceId) throws IOException {
        traceId = traceId != null ? traceId.trim() : null;
        traceId = Span.normalizeTraceId(traceId);
        List<Span> trace = storage.traces().getTrace(traceId).execute();
        if (trace.isEmpty()) {
            return AggregatedHttpResponse.of(NOT_FOUND, ANY_TEXT_TYPE, traceId + " not found");
        }
        return jsonResponse(SpanBytesEncoder.JSON_V2.encodeList(trace));
    }

    @Get("/api/v2/traceMany")
    @Blocking
    public AggregatedHttpResponse getTraces(@Param("traceIds") String traceIds) throws IOException {
        if (traceIds.isEmpty()) {
            return AggregatedHttpResponse.of(BAD_REQUEST, ANY_TEXT_TYPE, "traceIds parameter is empty");
        }

        Set<String> normalized = new LinkedHashSet<>();
        for (String traceId : traceIds.split(",", 1000)) {
            if (normalized.add(Span.normalizeTraceId(traceId))) continue;
            return AggregatedHttpResponse.of(BAD_REQUEST, ANY_TEXT_TYPE, "redundant traceId: " + traceId);
        }

        if (normalized.size() == 1) {
            return AggregatedHttpResponse.of(BAD_REQUEST, ANY_TEXT_TYPE,
                    "Use /api/v2/trace/{traceId} endpoint to retrieve a single trace");
        }

        List<List<Span>> traces = storage.traces().getTraces(normalized).execute();
        return jsonResponse(writeTraces(SpanBytesEncoder.JSON_V2, traces));
    }

    static AggregatedHttpResponse jsonResponse(byte[] body) {
        return AggregatedHttpResponse.of(ResponseHeaders.builder(200)
                .contentType(MediaType.JSON)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, body.length).build(), HttpData.wrap(body));
    }

    @Get("/api/v2/autocompleteKeys")
    @Blocking
    public AggregatedHttpResponse getAutocompleteKeys(ServiceRequestContext ctx) {
        return maybeCacheNames(true, autocompleteKeys, ctx.alloc());
    }

    @Get("/api/v2/autocompleteValues")
    @Blocking
    public AggregatedHttpResponse getAutocompleteValues(
            @Param("key") String key, ServiceRequestContext ctx) throws IOException {
        List<String> values = storage.autocompleteTags().getValues(key).execute();
        return maybeCacheNames(values.size() > 3, values, ctx.alloc());
    }

    /**
     * We cache names if there are more than 3 names. This helps people getting started: if we cache
     * empty results, users have more questions. We assume caching becomes a concern when zipkin is in
     * active use, and active use usually implies more than 3 services.
     */
    AggregatedHttpResponse maybeCacheNames(
            boolean shouldCacheControl, List<String> values, ByteBufAllocator alloc) {
        Collections.sort(values);
        int sizeEstimate = 2; // Two brackets.
        for (String value : values) {
            sizeEstimate += value.length() + 1 /* comma */;
        }
        sizeEstimate -= 1; // Last element doesn't have a comma.
        // If the values don't require escaping, this buffer will not be resized.
        ByteBuf buf = alloc.buffer(sizeEstimate);
        try (JsonGenerator gen =
                     JsonUtil.JSON_FACTORY.createGenerator((OutputStream) new ByteBufOutputStream(buf))) {
            gen.writeStartArray(values.size());
            for (String value : values) {
                gen.writeString(value);
            }
            gen.writeEndArray();
        } catch (IOException e) {
            buf.release();
            throw new UncheckedIOException(e);
        }
        ResponseHeadersBuilder headers = ResponseHeaders.builder(200)
                .contentType(MediaType.JSON)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, buf.readableBytes());
        if (shouldCacheControl) {
            headers = headers.add(CACHE_CONTROL, "max-age=" + namesMaxAge + ", must-revalidate");
        }
        return AggregatedHttpResponse.of(headers.build(), HttpData.wrap(buf));
    }

    // This is inlined here as there isn't enough re-use to warrant it being in the zipkin2 library
    static byte[] writeTraces(SpanBytesEncoder codec, List<List<zipkin2.Span>> traces) {
        // Get the encoded size of the nested list so that we don't need to grow the buffer
        int length = traces.size();
        int sizeInBytes = 2; // []
        if (length > 1) sizeInBytes += length - 1; // comma to join elements

        for (int i = 0; i < length; i++) {
            List<zipkin2.Span> spans = traces.get(i);
            int jLength = spans.size();
            sizeInBytes += 2; // []
            if (jLength > 1) sizeInBytes += jLength - 1; // comma to join elements
            for (int j = 0; j < jLength; j++) {
                sizeInBytes += codec.sizeInBytes(spans.get(j));
            }
        }

        byte[] out = new byte[sizeInBytes];
        int pos = 0;
        out[pos++] = '['; // start list of traces
        for (int i = 0; i < length; i++) {
            pos += codec.encodeList(traces.get(i), out, pos);
            if (i + 1 < length) out[pos++] = ',';
        }
        out[pos] = ']'; // stop list of traces
        return out;
    }
}
