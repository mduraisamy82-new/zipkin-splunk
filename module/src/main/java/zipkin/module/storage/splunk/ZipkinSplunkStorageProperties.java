/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.storage.splunk;

import java.io.Serializable;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.storage.splunk")
public class ZipkinSplunkStorageProperties implements Serializable {

    private static final long serialVersionUID = 0L;

    private String scheme;
    private String host;
    private int port;
    private String token;
    private String indexName;
    private String sourceType;
    private String source;
    private String dataModel;

    public String getScheme() {

        return scheme;
    }

    public void setScheme(String scheme) {

        this.scheme = scheme;
    }

    public String getHost() {

        return host;
    }

    public void setHost(String host) {

        this.host = host;
    }

    public int getPort() {

        return port;
    }

    public void setPort(int port) {

        this.port = port;
    }

    public String getIndexName() {

        return indexName;
    }

    public void setIndexName(String indexName) {

        this.indexName = indexName;
    }

    public String getSourceType() {

        return sourceType;
    }

    public void setSourceType(String sourceType) {

        this.sourceType = sourceType;
    }

    public String getSource() {

        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDataModel() {
        return dataModel;
    }

    public void setDataModel(String dataModel) {
        this.dataModel = dataModel;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}