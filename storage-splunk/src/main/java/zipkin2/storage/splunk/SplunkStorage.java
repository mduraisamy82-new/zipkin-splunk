/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.splunk;

import com.splunk.Args;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

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
        this.serviceArgs.setUsername(builder.username);
        this.serviceArgs.setPassword(builder.password);
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

    @Override public SpanConsumer spanConsumer() {
        return spanConsumer;
    }

    @Override public ServiceAndSpanNames serviceAndSpanNames() {
        return  serviceAndSpanNames;
    }

    Service splunk() {
        if (splunk == null) {
            synchronized (this) {
                if (splunk == null) {
                    if(serviceArgs.token !=null ) {
                        LOG.debug("Connected using Token");
                        this.splunk = new Service(serviceArgs);
                    }else{
                        LOG.debug("Connected using UserName & Password");
                        this.splunk = Service.connect(serviceArgs);
                    }
                }
            }
        }
        return splunk;
    }

    public static class Builder extends StorageComponent.Builder {

        String scheme = "https";
        String host = "localhost";
        int port = 8089;
        String username;
        String password;
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

        public Builder username(String username) {
            if (username == null) throw new NullPointerException("username == null");
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            if (password == null) throw new NullPointerException("password == null");
            this.password = password;
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