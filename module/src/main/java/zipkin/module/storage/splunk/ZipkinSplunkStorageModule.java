/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.storage.splunk;

import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.RedirectService;
import com.linecorp.armeria.server.annotation.Order;
import com.linecorp.armeria.spring.ArmeriaServerConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.splunk.SplunkStorage;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(ZipkinSplunkStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "splunk")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinSplunkStorageModule {

    @Autowired
    ZipkinSplunkStorageProperties storageProperties;

    @Bean
    @ConditionalOnMissingBean
    StorageComponent storage(ZipkinSplunkStorageProperties properties) {
        return SplunkStorage.builder()
                .host(properties.getHost())
                .port(properties.getPort())
                .scheme(properties.getScheme())
                .username(properties.getUsername())
                .password(properties.getPassword())
                .indexName(properties.getIndexName())
                .source(properties.getSource())
                .sourceType(properties.getSourceType())
                .dataModel(properties.getDataModel())
                .token(properties.getToken())
                .build();
    }


    @Bean
    @ConditionalOnProperty(name = "zipkin.storage.type.ui", havingValue = "custom")
    ArmeriaServerConfigurator zipkinServerConfigurator() {
        return sb -> {
            sb.service("/zipkin/static/media/zipkin-logo.png", new RedirectService(HttpStatus.FOUND, "https://mydhl.express.dhl/content/dam/ewf/logos/dhl_express_logo_transparent.png"));
        };
    }

}


