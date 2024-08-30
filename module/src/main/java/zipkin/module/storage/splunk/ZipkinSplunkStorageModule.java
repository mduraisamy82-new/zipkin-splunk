/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.storage.splunk;

import com.linecorp.armeria.common.*;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.RedirectService;
import com.linecorp.armeria.server.file.FileService;
import com.linecorp.armeria.server.file.HttpFile;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.spring.ArmeriaServerConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.EnableScheduling;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.splunk.SplunkStorage;
import zipkin2.storage.splunk.internal.SplunkExportTrace;
import zipkin2.storage.splunk.internal.ZipkinSplunkQueryApiV2;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(ZipkinSplunkStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "splunk")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinSplunkStorageModule {

    @Autowired
    ZipkinSplunkStorageProperties storageProperties;

    @Value("classpath:static/login.html")
    Resource loginHtml;


    @Bean
    @ConditionalOnMissingBean
    StorageComponent storage(ZipkinSplunkStorageProperties properties) {
        return SplunkStorage.builder()
                .host(properties.getHost())
                .port(properties.getPort())
                .scheme(properties.getScheme())
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
                                                       @Value("${zipkin.query.timeout:11s}") Duration queryTimeout,
                                                       HttpService indexService) {
        ServerCacheControl maxAgeYear =
                ServerCacheControl.builder().maxAgeSeconds(TimeUnit.DAYS.toSeconds(365)).build();

        HttpService uiFileService = FileService.builder(getClass().getClassLoader(), "zipkin-lens")
                .cacheControl(maxAgeYear)
                .build();

        HttpFile loginPage = HttpFile.of(getClass().getClassLoader(),"static/login.html");
        return sb -> {

            zipkinSplunkQueryApiV2.ifPresent(h -> {
                Function<HttpService, HttpService>
                        timeoutDecorator = delegate -> (ctx, req) -> {
                    System.out.println("Inside timeoutDecorator "+req.path());
                    ctx.setRequestTimeout(queryTimeout);
                    return delegate.serve(ctx, req);
                };

                Function<HttpService, HttpService>
                        authenticationDecorator = delegate -> (ctx, req) -> {
                    System.out.println("Inside authenticationDecorator "+req.path());
                    Cookie sptokenCookie = req.headers().cookies().stream().
                            filter(cookie -> cookie.name().equalsIgnoreCase("sptoken")).
                            findFirst().
                            orElse(null);
                    if(sptokenCookie == null && (req.path().contains("/trace") && !req.path().equalsIgnoreCase("/trace/login"))){
                        return HttpResponse.ofRedirect(HttpStatus.FOUND,"/login/");
                    }
                    return delegate.serve(ctx, req);
                };

                Function<HttpService, HttpService> authenticatedWithTimeout = timeoutDecorator.andThen(authenticationDecorator);

                sb.serviceUnder("/trace/", uiFileService.decorate(authenticatedWithTimeout));

                sb.service("/trace/", indexService)
                        .service("/trace/index.html", indexService)
                        .service("/trace/traces/{id}", indexService)
                        .service("/trace/dependency", indexService)
                        .service("/trace/traceViewer", indexService);


                sb.annotatedService("/trace", zipkinSplunkQueryApiV2.get(), timeoutDecorator).decorator(authenticationDecorator); // For UI.
                // sb.service("/zipkin/static/media/zipkin-logo.png", new RedirectService(HttpStatus.FOUND, "https://mydhl.express.dhl/content/dam/ewf/logos/dhl_express_logo_transparent.png"));
                sb.service("/trace/config.json", new RedirectService(HttpStatus.FOUND, "/zipkin/config.json")) .
                        service("/", new RedirectService(HttpStatus.FOUND, "/trace/")) .
                        service("/trace", new RedirectService(HttpStatus.FOUND, "/trace/"))
                ;

                //login file
                sb.service("/login/", loginPage.asService());

                //Grpc
                sb.service(GrpcService.builder()
                        .addService(new SplunkExportTrace(null,null))
                        .build());

            });
        };
    }

}


