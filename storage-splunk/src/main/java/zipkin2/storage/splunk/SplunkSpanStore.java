/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk;

import com.splunk.Event;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;

import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.storage.splunk.SplunkStorage.DECODER;

public class SplunkSpanStore implements SpanStore, ServiceAndSpanNames {

    static final Logger LOG = LoggerFactory.getLogger(SplunkSpanStore.class);

    final SplunkStorage storage;

    SplunkSpanStore(SplunkStorage storage) {
        this.storage = storage;
    }

    @Override public Call<List<String>> getServiceNames() {
        LOG.debug("getServiceNames: {}");
        final String query = "search * index=\"" + storage.indexName + "\" "
                + " sourcetype=" + storage.sourceType + ""
                + "| table localEndpoint.serviceName "
                + "| dedup localEndpoint.serviceName";
        return new GetNamesCall(storage, query, "localEndpoint.serviceName");
    }

    @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
        LOG.debug("getRemoteServiceNames: {}");
        final String query = "search * index=\"" + storage.indexName + "\" "
                + "localEndpoint " + serviceName + " "
                + "| table remoteEndpoint.serviceName "
                + "| dedup remoteEndpoint.serviceName";
        return new GetNamesCall(storage, query, "remoteEndpoint.serviceName");
    }

    @Override public Call<List<String>> getSpanNames(String serviceName) {
        LOG.debug("getSpanNames: {}",serviceName);
        final String query = "search * index=\"" + storage.indexName + "\" "
                + "localEndpoint " + serviceName + " "
                + "| table name "
                + "| dedup name";
        return new GetNamesCall(storage, query, "name");
    }

    @Override public Call<List<List<Span>>> getTraces(QueryRequest request)
    {
        LOG.debug("getTraces query: {}", request);
        String startQuery = "search * index=" + storage.indexName
                + " sourcetype=" + storage.sourceType + ""
                + " earliest=" + (request.lookback() / 1000) + ""
                + " latest=" + (request.endTs() / 1000) + ""
                + " | transaction traceId | ";
        String endQuery = " head " + request.limit();
        StringBuilder queryBuilder = new StringBuilder(startQuery);
        if (request.serviceName() != null && !request.serviceName().equalsIgnoreCase("all")) {
            queryBuilder.append(" where \'localEndpoint.serviceName\' = \"")
                    .append(request.serviceName())
                    .append("\" | ");
        }
        if (request.spanName() != null && !request.spanName().equalsIgnoreCase("all")) {
            queryBuilder.append("  where name = \"")
                    .append(request.spanName())
                    .append("\" | ");
        }
        if (request.remoteServiceName() != null && !request.remoteServiceName()
                .equalsIgnoreCase("all")) {
            queryBuilder.append(" where \'remoteEndpoint.serviceName\' = \"")
                    .append(request.remoteServiceName())
                    .append("\" | ");
        }
        for (Map.Entry<String, String> tag : request.annotationQuery().entrySet()) {
            queryBuilder.append(" where \'tags.").append(tag.getKey()).append("\' = \"")
                    .append(tag.getValue())
                    .append("\" | ");
        }
        if (request.minDuration() != null) {
            queryBuilder.append(" where duration > ")
                    .append(request.minDuration())
                    .append(" | ");
        }
        if (request.maxDuration() != null) {
            queryBuilder.append(" where duration < ")
                    .append(request.maxDuration())
                    .append(" | ");
        }
        queryBuilder.append(endQuery);
        final String query = queryBuilder.toString();
        LOG.debug("getTraces query: {}", query);
        return new GetTracesCall(storage, query);
    }

    @Override public Call<List<Span>> getTrace(String traceId) {
        LOG.debug("getTrace: {}", traceId);
        final String query = "search * index=\"" + storage.indexName + "\" "
                + "sourcetype=\"" + storage.sourceType + "\" "
                + "traceid " + traceId;
        LOG.debug("getTrace query: {}", query);
        return new GetTraceCall(storage, query, traceId);
    }

    // -------------------------------------------------------------------------------------------

    // -------------------------------------------------------------------------------------------------------------

    static class GetTracesCall extends RawSplunkSearchCall<List<Span>> {

        GetTracesCall(SplunkStorage storage, String query) {
            super(storage, query);
        }

        GetTracesCall(Service splunkService, String query) {
            super(splunkService, query);
        }

        @Override List<List<Span>> process(ResultsReaderXml results) {
            LOG.debug("process: {}", results);
            List<List<Span>> traces = new ArrayList<>();
            for (Event event : results) {
                String[] raws = event.get("_raw").split("\\n");
                List<Span> trace = new ArrayList<>();
                for (String raw : raws) {
                    byte[] bytes = raw.getBytes(UTF_8);
                    Span span = DECODER.decodeOne(bytes);
                    trace.add(span);
                }
                traces.add(trace);
            }
            LOG.debug("process: {}", traces);
            return traces;
        }

        @Override public Call<List<List<Span>>> clone() {
            return new GetTracesCall(splunk, query);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------


    static class GetTraceCall extends SplunkSearchCall<Span> {
        final String traceId;

        GetTraceCall(SplunkStorage storage, String query, String traceId) {
            super(storage, query);
            this.traceId = traceId;
        }

        GetTraceCall(Service SplunkService, String query, String traceId) {
            super(SplunkService, query);
            this.traceId = traceId;
        }

        @Override Span parse(Event event) {
            final String raw = event.get("_raw");
            final byte[] bytes = raw.getBytes(UTF_8);
            return DECODER.decodeOne(bytes);
        }

        @Override public Call<List<Span>> clone() {
            return new GetTraceCall(splunk, query, traceId);
        }
    }

    // ------------------------------------------------------------------------------------------



    static class GetNamesCall extends SplunkSearchCall<String> {
        final String fieldName;

        GetNamesCall(SplunkStorage storage, String query, String fieldName) {
            super(storage, query);
            this.fieldName = fieldName;
        }

        GetNamesCall(Service SplunkService, String query, String fieldName) {
            super(SplunkService, query);
            this.fieldName = fieldName;
        }

        @Override String parse(Event event) {
            return event.get(fieldName);
        }

        @Override public Call<List<String>> clone() {
            return new GetNamesCall(splunk, query, fieldName);
        }
    }

    // ---------------------------------------------------------------------------------------------------------

    @Override public Call<List<DependencyLink>> getDependencies(long start, long end) {
        return null;
    }

    // ---------------------------------------------------------------------------------------------------------

    static abstract class SplunkSearchCall<T> extends RawSplunkSearchCall<T> {

        SplunkSearchCall(SplunkStorage storage, String query) {
            super(storage, query);
        }

        SplunkSearchCall(Service SplunkService, String query) {
            super(SplunkService, query);
        }

        @Override List<T> process(ResultsReaderXml results) {
            List<T> list = new ArrayList<>();
            for (Event event : results) {
                T item = parse(event);
                list.add(item);
            }
            return list;
        }

        abstract T parse(Event event);
    }

    // ---------------------------------------------------------------------------------------------------------

    static abstract class RawSplunkSearchCall<T> extends Call.Base<List<T>> {
        final SplunkStorage storage;
        final Service splunk;
        final String query;

        RawSplunkSearchCall(SplunkStorage storage, String query) {
            this.storage = storage;
            this.splunk = storage.splunk();
            this.query = query;
        }

        RawSplunkSearchCall(Service SplunkService, String query) {
            this.storage = null;
            this.splunk = SplunkService;
            this.query = query;
        }


        @Override protected List<T> doExecute() throws IOException {
            InputStream is =  splunk.oneshotSearch(query);
            ResultsReaderXml xml = new ResultsReaderXml(is);
            return process(xml);
        }

        @Override protected void doEnqueue(Callback<List<T>> callback) {
            LOG.debug("doEnqueue {}",callback);
            try (InputStream is = splunk.oneshotSearch(query)) {
                ResultsReaderXml xml = new ResultsReaderXml(is);
                callback.onSuccess(process(xml));
            } catch (Exception e) {
                e.printStackTrace();
                callback.onError(e);
            }
        }

        abstract List<T> process(ResultsReaderXml results);
    }

    // -----------------------------------------------------------------------------------------------------------
}
