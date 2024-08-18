/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.storage.splunk;

import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.RedirectService;
import com.linecorp.armeria.spring.ArmeriaServerConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.splunk.SplunkStorage;
import zipkin2.storage.splunk.internal.ZipkinSplunkQueryApiV2;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

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
    @ConditionalOnMissingBean
    ZipkinSplunkQueryApiV2 zipkinSplunkQueryApiV2(StorageComponent storage,
                                                  @Value("${zipkin.storage.type:mem}") String storageType,
                                                  @Value("${zipkin.query.lookback:86400000}") long defaultLookback,
                                                  @Value("${zipkin.query.names-max-age:300}") int namesMaxAge,
                                                  @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys
                                        ){
        return new
                ZipkinSplunkQueryApiV2(storage,storageType,defaultLookback,namesMaxAge,autocompleteKeys);
    }


    @Bean
    @ConditionalOnMissingBean
    ArmeriaServerConfigurator zipkinServerConfigurator(Optional<ZipkinSplunkQueryApiV2> zipkinSplunkQueryApiV2,
                                                       @Value("${zipkin.query.timeout:11s}") Duration queryTimeout) {
        System.out.println("zipKinSplunkApiV2" + zipkinSplunkQueryApiV2.isPresent());
        return sb -> {
            zipkinSplunkQueryApiV2.ifPresent(h -> {
                Function<HttpService, HttpService>
                        timeoutDecorator = service -> (ctx, req) -> {
                    ctx.setRequestTimeout(queryTimeout);
                    return service.serve(ctx, req);
                };
                sb.annotatedService(zipkinSplunkQueryApiV2.get(), timeoutDecorator);
                sb.annotatedService("/zipkin", zipkinSplunkQueryApiV2.get(), timeoutDecorator); // For UI.
                sb.service("/zipkin/static/media/zipkin-logo.png", new RedirectService(HttpStatus.FOUND, "https://mydhl.express.dhl/content/dam/ewf/logos/dhl_express_logo_transparent.png"));
            });
            //
        };
    }

}


