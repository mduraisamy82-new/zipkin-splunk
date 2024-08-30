/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk;

import com.splunk.Event;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.BytesDecoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.splunk.internal.OtelSpanBytesDecoder;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SplunkOtelSpanStore extends SplunkSpanStore{

    static final Logger LOG = LoggerFactory.getLogger(SplunkOtelSpanStore.class);

    static final BytesDecoder<Span> DECODER = new OtelSpanBytesDecoder();

    private final long defaultLookback;

    private Call<List<String>> serviceNames;

    private Call<List<String>> remoteServiceNames;

    final String splunkToken;

    SplunkOtelSpanStore(SplunkStorage storage, long defaultLookback, String splunkToken) {
        super(storage);
        this.defaultLookback = defaultLookback;
        this.splunkToken = splunkToken;
    }

    SplunkOtelSpanStore(SplunkStorage storage, long defaultLookback) {
        this(storage,defaultLookback,null);
    }

    // To test
    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
        LOG.debug("getTraces query: {}", request);
        String startQuery = "search * index=\"" + storage.indexName + "\" "
                + " sourcetype=\"" + storage.sourceType + "\" "
                + " earliest=-" + (request.lookback() / 1000) + ""
                + " latest=" + (request.endTs() / 1000) + ""
                + " | transaction scopeSpans{}.spans{}.traceId | ";
        String endQuery = " sort -scopeSpans{}.spans{}.startTimeUnixNano | head " + request.limit();
        StringBuilder queryBuilder = new StringBuilder(startQuery);
        if (request.serviceName() != null && !request.serviceName().equalsIgnoreCase("all")) {
            queryBuilder.append(" search \"resource.attributes{}.value.stringValue\" = \"")
                    .append(request.serviceName())
                    .append("\" | ");
        }
        if (request.spanName() != null && !request.spanName().equalsIgnoreCase("all")) {
            queryBuilder.append(" search \"scopeSpans{}.spans{}.name\" = \"")
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
        return new GetTracesCall(storage.getSplunkService(splunkToken),query);
    }

    @Override public Call<List<Span>> getTrace(String traceId) {
        LOG.debug("getTrace: {}", traceId);
        final String query = "search * index=\"" + storage.indexName + "\" "
                + "sourcetype=\"" + storage.sourceType + "\" "
                + "scopeSpans{}.spans{}.traceId = " + traceId;
        LOG.debug("getTrace query: {}", query);
        return new GetTraceCall(storage.getSplunkService(splunkToken), query, traceId);
    }


    @Override public Call<List<String>> getServiceNames() {
        LOG.debug("getServiceNames {}",this.serviceNames);
        final String query = getServiceNamesQueryBuilder();
        LOG.debug("getServiceNames query: {}", query);
        return new GetNamesCall(storage.getSplunkService(splunkToken), query, "serviceName");
    }

    // To change
    protected String getServiceNamesQueryBuilder() {
        StringBuilder query = new StringBuilder();
        query.append("search * index=\"");
        query.append(storage.indexName);
        query.append("\" ");
        query.append(" sourcetype=\"" );
        query.append(storage.sourceType);
        query.append("\" ");
        if(defaultLookback >0 ) {
            query.append("earliest = -");
            query.append(defaultLookback/1000);
            query.append("s");
        }
        query.append(" | spath path=resource.attributes{} output=attributesmv  ");
        query.append(" | eval index = mvfind(attributesmv, \"service.name\")  ");
        query.append(" | eval snamejson=mvindex(attributesmv,index) ");
        query.append(" | spath path=value.stringValue input=snamejson output=serviceName");
        query.append(" | fields serviceName | table serviceName | dedup serviceName");
        return query.toString();
    }

    //To do

    @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
        LOG.debug("getRemoteServiceNames");
        final String query = "search * index=\"" + storage.indexName + "\" "
                + " sourcetype=\"" + storage.sourceType + "\" "
                + "| eval serviceName=mvindex('resource.attributes{}.value.stringValue', 10) "
                + "| table serviceName "
                + "| dedup serviceName";
        LOG.debug("getRemoteServiceNames query {}",query);
        return new GetNamesCall(storage.getSplunkService(splunkToken), query, "serviceName");
    }

    // All good
    @Override public Call<List<String>> getSpanNames(String serviceName) {
        LOG.debug("getSpanNames: {}",serviceName);
        final String query = "search * index=\"" + storage.indexName + "\" "
                + " sourcetype=\"" + storage.sourceType + "\" "
                + "resource.attributes{}.value.stringValue = " + serviceName + " "
                + "| table scopeSpans{}.spans{}.name "
                + "| dedup scopeSpans{}.spans{}.name ";
        LOG.debug("getSpanNames query {}",query);
        return new GetNamesCall(storage.getSplunkService(splunkToken), query, "scopeSpans{}.spans{}.name");
    }

    static class GetTracesCall extends SplunkSpanStore.GetTracesCall {

        GetTracesCall(Service splunkService, String query) {
            super(splunkService, query);
        }

        @Override
        List<List<Span>> process(ResultsReaderXml results) {
            LOG.debug("process: {}", results);
            List<List<Span>> traces = new ArrayList<>();
            for (Event event : results) {
                String[] raws = event.get("_raw").split("\\n");
                try {
                    List<Span> trace = new ArrayList<>();
                    for (String raw : raws) {
                        byte[] bytes = raw.getBytes(UTF_8);
                        Span span = DECODER.decodeOne(bytes);
                        trace.add(span);
                    }
                    traces.add(trace);
                }catch(RuntimeException exception){
                    LOG.error("Exception while decoding a trace",exception);
                }
            }
            LOG.trace("process: {}", traces);
            return traces;
        }

        @Override public Call<List<List<Span>>> clone() {
            return new SplunkOtelSpanStore.GetTracesCall(splunk, query);
        }
    }

    static class GetTraceCall extends SplunkSearchCall<Span> {
        final String traceId;


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
            return new SplunkOtelSpanStore.GetTraceCall(splunk, query, traceId);
        }
    }

    static class GetDependencyLinkCall extends SplunkSearchCall<DependencyLink> {
        final long start;
        final long end;


        GetDependencyLinkCall(Service SplunkService, String query, long start ,long end) {
            super(SplunkService, query);
            this.start = start;
            this.end = end;
        }


        @Override
        DependencyLink parse(Event event) {
            DependencyLink dependencyLink = null;
            if(event.get("scopeSpans{}.spans{}.kind").equalsIgnoreCase("5")){
                dependencyLink = DependencyLink.newBuilder().child(event.get("parent")).
                        parent(event.get("child")).
                        callCount(Long.parseLong(event.get("callcount"))).errorCount(0L).build();
            }else{
                 dependencyLink = DependencyLink.newBuilder().parent(event.get("parent")).
                        child(event.get("child")).
                        callCount(Long.parseLong(event.get("callcount"))).errorCount(0L).build();
            }
            LOG.debug("DependencyLink: {} ",dependencyLink);
            return dependencyLink;
        }

        @Override
        public Call<List<DependencyLink>> clone() {
            return new GetDependencyLinkCall(splunk,query,start,end);
        }
    }

    @Override public Call<List<DependencyLink>> getDependencies(long start, long end) {
        LOG.debug("getDependencies: {} {}", start,end);
        Character space = ' ';
        Character pipe = '|';
        StringBuilder queryBuilder = new StringBuilder("search * index=");
        queryBuilder.append(storage.indexName);
        queryBuilder.append(space);
        queryBuilder.append("sourcetype=");
        queryBuilder.append(storage.sourceType);
        queryBuilder.append(space);
        queryBuilder.append("earliest=-86400s");
        queryBuilder.append(space);
        queryBuilder.append("scopeSpans{}.spans{}.kind IN (3,4,5)");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("spath path=resource.attributes{} output=attributesmv");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("spath path=scopeSpans{}.spans{}.attributes{} output=scopeSpansmv");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval index = mvfind(attributesmv, \"service.name\")");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval snamejson=mvindex(attributesmv,index)");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("spath path=value.stringValue input=snamejson output=parent");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval indexdb = mvfind(scopeSpansmv, \"db.name\")");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval dbnamejson=mvindex(scopeSpansmv,indexdb)");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("spath path=value.stringValue input=dbnamejson output=dbname");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval indexurl = mvfind(scopeSpansmv, \"url.full\")");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval urljson=mvindex(scopeSpansmv,indexurl)");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("spath path=value.stringValue input=urljson output=url");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval indexmsg = mvfind(scopeSpansmv, \"messaging.destination.name\")");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval msgjson=mvindex(scopeSpansmv,indexmsg)");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("spath path=value.stringValue input=msgjson output=msg");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("eval child = coalesce(url,dbname,msg)");
        queryBuilder.append(space);
        queryBuilder.append(pipe);
        queryBuilder.append("stats count as callcount by parent child scopeSpans{}.spans{}.kind");
        final String query = queryBuilder.toString();
        LOG.debug("getTraces query: {}", query);
        return new GetDependencyLinkCall(storage.getSplunkService(splunkToken),query,start,end);
    }

}
