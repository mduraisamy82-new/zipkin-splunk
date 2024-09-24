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
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.BytesDecoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.splunk.internal.OtelSpanBytesDecoder;
import zipkin2.storage.splunk.internal.OtelSpanBytesDefaultDecoder;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SplunkOtelSpanStore extends SplunkSpanStore{

    static final Logger LOG = LoggerFactory.getLogger(SplunkOtelSpanStore.class);

    static final BytesDecoder<Span> DECODER = new OtelSpanBytesDefaultDecoder();

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
                + " | transaction trace_id | ";
        String endQuery = " sort -start_time | fields _raw,service.name | head " + request.limit();
        StringBuilder queryBuilder = new StringBuilder(startQuery);
        if (request.serviceName() != null && !request.serviceName().equalsIgnoreCase("all")) {
            queryBuilder.append(" search \"service.name\" = \"")
                    .append(request.serviceName())
                    .append("\" | ");
        }
        if (request.spanName() != null && !request.spanName().equalsIgnoreCase("all")) {
            queryBuilder.append(" search \"name\" = \"")
                    .append(request.spanName())
                    .append("\" | ");
        }
        if (request.remoteServiceName() != null && !request.remoteServiceName()
                .equalsIgnoreCase("all")) {
            queryBuilder.append(" where \'serviceName\' = \"")
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
                    .append((request.minDuration()/1000.0))
                    .append(" | ");
        }
        if (request.maxDuration() != null) {
            queryBuilder.append(" where duration < ")
                    .append((request.maxDuration()/1000.0))
                    .append(" | ");
        }
        queryBuilder.append(endQuery);
        final String query =  queryBuilder.toString();
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
        return new GetNamesCall(storage.getSplunkService(splunkToken), query, "service.name");
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
        query.append(" | table service.name ");
        query.append(" | dedup service.name");
        return query.toString();
    }

    //To do

    @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
        LOG.debug("getRemoteServiceNames");
        final String query = getRemoteNamesQueryBuilder(serviceName);
        LOG.debug("getRemoteServiceNames query: {}", query);
        return new GetNamesCall(storage.getSplunkService(splunkToken), query, "remoteServiceName");
    }

    protected String getRemoteNamesQueryBuilder(String serviceName) {
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
        query.append(" scopeSpans{}.spans{}.kind IN (3,4,5)");
        query.append(" resource.attributes{}.value.stringValue = ");
        query.append(serviceName);
        query.append(" | spath path=scopeSpans{}.spans{}.attributes{} output=scopeSpansmv");
        query.append(" | eval indexdb = mvfind(scopeSpansmv, \"db.name\")");
        query.append(" | eval dbnamejson=mvindex(scopeSpansmv,indexdb)");
        query.append(" | spath path=value.stringValue input=dbnamejson output=dbname");
        query.append(" |eval indexurl = mvfind(scopeSpansmv, \"url.full\")");
        query.append(" |eval urljson=mvindex(scopeSpansmv,indexurl)");
        query.append(" |spath path=value.stringValue input=urljson output=url");
        query.append(" |eval indexmsg = mvfind(scopeSpansmv, \"messaging.destination.name\")");
        query.append(" |eval msgjson=mvindex(scopeSpansmv,indexmsg)");
        query.append(" |spath path=value.stringValue input=msgjson output=msg");
        query.append(" |eval remoteServiceName = coalesce(url,dbname,msg)");
        query.append(" |fields remoteServiceName | table remoteServiceName | dedup remoteServiceName");
        return query.toString();
    }

    // All good
    @Override public Call<List<String>> getSpanNames(String serviceName) {
        LOG.debug("getSpanNames: {}",serviceName);
        final String query = "search * index=\"" + storage.indexName + "\" "
                + "sourcetype=\"" + storage.sourceType + "\" "
                + "service.name = " + serviceName + " "
                + "| table name "
                + "| dedup name ";
        LOG.debug("getSpanNames query {}",query);
        return new GetNamesCall(storage.getSplunkService(splunkToken), query, "name");
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
        LOG.debug("Dependencies query: {}", query);
        return new GetDependencyLinkCall(storage.getSplunkService(splunkToken),query,start,end);
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
                LOG.debug("Event {}",event);
                String[] raws = event.get("_raw").split("\\n");
                String serviceName = event.get("service.name");
                try {
                    List<Span> trace = new ArrayList<>();
                    for (String raw : raws) {
                        byte[] bytes = raw.getBytes(UTF_8);
                        Span span = DECODER.decodeOne(bytes);
                        trace.add(span.toBuilder().localEndpoint(
                                    Endpoint.newBuilder().serviceName(serviceName).build()).
                                    build()
                                );
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
            LOG.debug("parse: {}", event);
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



}
