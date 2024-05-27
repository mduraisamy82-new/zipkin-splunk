/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.storage.splunk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.splunk.SplunkStorage;

@Configuration
@EnableConfigurationProperties(ZipkinSplunkStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "splunk")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinSplunkStorageModule {

    @Autowired
    ZipkinSplunkStorageProperties storageProperties;

    @Bean
    @ConditionalOnMissingBean
    StorageComponent storage(ZipkinSplunkStorageProperties properties) {
        return SplunkStorage.newBuilder()
                .host(properties.getHost())
                .port(properties.getPort())
                .scheme(properties.getScheme())
                .username(properties.getUsername())
                .password(properties.getPassword())
                .indexName(properties.getIndexName())
                .source(properties.getSource())
                .sourceType(properties.getSourceType()).build();
    }

}


