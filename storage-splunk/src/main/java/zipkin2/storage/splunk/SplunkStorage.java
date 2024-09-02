/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk;

import com.splunk.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.splunk.internal.LoginForm;

public class SplunkStorage extends StorageComponent {

    static final Logger LOG = LoggerFactory.getLogger(SplunkStorage.class);

    static final SpanBytesDecoder DECODER =SpanBytesDecoder.JSON_V2;
    static final SpanBytesEncoder ENCODER = SpanBytesEncoder.JSON_V2;

    final ServiceArgs serviceArgs;
    final String indexName;
    final Args indexArgs;
    final String sourceType;

    final SpanStore spanStore;
    final SpanConsumer spanConsumer;
    final ServiceAndSpanNames serviceAndSpanNames;

    volatile Service splunk;

    SplunkStorage(Builder builder) {
        this.serviceArgs = new ServiceArgs();
        this.serviceArgs.setHost(builder.host);
        this.serviceArgs.setPort(builder.port);
        if(builder.token !=null && !builder.token.isEmpty() && !builder.token.equalsIgnoreCase("_")) {
            this.serviceArgs.setToken("Splunk " + builder.token);
        }
        this.serviceArgs.setScheme(builder.scheme);
        this.serviceArgs.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
        this.indexName = builder.indexName;

        this.indexArgs = new Args();
        this.indexArgs.add("source", builder.source);
        this.indexArgs.add("sourcetype", builder.sourceType);

        this.sourceType = builder.sourceType;
        LOG.debug("dataModel: {}", builder.dataModel);
        if(builder.dataModel.equalsIgnoreCase("otel")){
            LOG.debug("Instatiate for otel: {}", builder.dataModel);
            this.spanConsumer =  new SplunkSpanConsumer(this);
            this.spanStore = new SplunkOtelSpanStore(this,builder.defaultLookBack);
            this.serviceAndSpanNames = new SplunkOtelSpanStore(this,builder.defaultLookBack);
        }else{
            LOG.debug("dataModel: {}", builder.dataModel);
            this.spanConsumer =  new SplunkSpanConsumer(this);
            this.spanStore = new SplunkSpanStore(this);
            this.serviceAndSpanNames = new SplunkSpanStore(this);
        }
    }



    @Override public SpanStore spanStore() {
        return spanStore;
    }

    public SpanStore spanStore(String token) {
       return  new SplunkOtelSpanStore(this,0L,token);
    }

    @Override public SpanConsumer spanConsumer() {
        return spanConsumer;
    }

    @Override public ServiceAndSpanNames serviceAndSpanNames() {
        return  serviceAndSpanNames;
    }

    public ServiceAndSpanNames serviceAndSpanNames(String token) {
        return  new SplunkOtelSpanStore(this,0L,token);
    }

    Service splunk() {
        if (splunk == null) {
            synchronized (this) {
                if (splunk == null) {
                    LOG.debug("Connected using Token");
                    this.splunk = new Service(serviceArgs);
                }
            }
        }
        return splunk;
    }

    public SplunkCollectorTrace getSplunkTraceExporter(){
        Index index= splunk().getIndexes().get(indexName);
        return new SplunkCollectorTrace(index,indexArgs);
    }

    public SplunkCollectorMetrics getSplunkMetricCollector(){
        Index index= splunk().getIndexes().get(indexName);

        Args indexArgs = new Args();
        indexArgs.add("source", this.indexArgs.get("source"));
        indexArgs.add("sourcetype", "mts");

        return new SplunkCollectorMetrics(index,indexArgs);
    }

    public Service getSplunkService(String token){
        ServiceArgs serviceArgsForToken = new ServiceArgs();
        serviceArgsForToken.setHost(this.serviceArgs.host);
        serviceArgsForToken.setPort(this.serviceArgs.port);
        serviceArgsForToken.setToken(token);
        serviceArgsForToken.setScheme(this.serviceArgs.scheme);
        serviceArgsForToken.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
        return new Service(serviceArgsForToken);
    }

    public Service login(LoginForm loginForm){
        serviceArgs.setUsername(loginForm.getUsername());
        serviceArgs.setPassword(loginForm.getPassword());
        Service svc =  Service.connect(serviceArgs);
        return svc;
    }

    public static class Builder extends StorageComponent.Builder {

        String scheme = "https";
        String host = "localhost";
        int port = 8089;
        String token;
        String indexName = "zipkin";
        String source = "zipkin-server";
        String sourceType = "span";
        String dataModel = "otel";
        long defaultLookBack = 86400000L;
        boolean strictTraceId = true;
        boolean searchEnabled = true;

        @Override public StorageComponent.Builder strictTraceId(boolean b) {
            this.strictTraceId = strictTraceId;
            return this;
        }

        @Override public StorageComponent.Builder searchEnabled(boolean b) {
            this.searchEnabled = searchEnabled;
            return this;
        }

        public Builder indexName(String indexName) {
            if (indexName == null) throw new NullPointerException("indexName == null");
            this.indexName = indexName;
            return this;
        }

        public Builder host(String host) {
            if (host == null) throw new NullPointerException("host == null");
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            if (port == 0) throw new NullPointerException("port == null");
            this.port = port;
            return this;
        }


        public Builder scheme(String scheme) {
            if (scheme == null) throw new NullPointerException("scheme == null");
            this.scheme = scheme;
            return this;
        }

        public Builder source(String source) {
            if (source == null) throw new NullPointerException("source == null");
            this.source = source;
            return this;
        }

        public Builder sourceType(String sourceType) {
            if (sourceType == null) throw new NullPointerException("sourceType == null");
            this.sourceType = sourceType;
            return this;
        }

        public Builder dataModel(String dataModel) {
            if (dataModel == null) throw new NullPointerException("dataModel == null");
            this.dataModel = dataModel;
            return this;
        }

        public Builder token(String token) {
            if (token == null) throw new NullPointerException("token == null");
            this.token = token;
            return this;
        }

        public Builder defaultLookback(long defaultLookBack) {
            if(defaultLookBack != 0L) {
                this.defaultLookBack = defaultLookBack;
            }
            return this;
        }

        @Override public SplunkStorage build() {

            return new SplunkStorage(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}