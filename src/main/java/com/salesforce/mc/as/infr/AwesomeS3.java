package com.salesforce.mc.as.infr;

import com.google.common.util.concurrent.RateLimiter;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Advanced S3 utilities
 * AwesomeS3.withBucket("some-bucket").directories.stream.
 * AwesomeS3.builder().bucket("bucket").filter().dateRange(start, end).forEach(Consumer<String> );
 */
public class AwesomeS3 {


    private final String bucket;
    private final int age;
    private final BlockingDeque<ListObjectsRequest> executerQueue;
    private final S3AsyncClient s3client;



    public AwesomeS3(String bucket, int age) {
        this.bucket = bucket;
        this.age = age;
        executerQueue = new LinkedBlockingDeque<>();
        s3client = S3AsyncClient.builder()
                //.httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(100).connectionAcquisitionTimeout(Duration.ofSeconds(30)))
                .overrideConfiguration(builder -> builder
                .apiCallTimeout(Duration.ofSeconds(30))
                .apiCallAttemptTimeout(Duration.ofSeconds(30))
                .retryPolicy(b -> b
                        .backoffStrategy(FullJitterBackoffStrategy.builder().baseDelay(Duration.ofSeconds(3)).maxBackoffTime(Duration.ofSeconds(60)).build())
                        .throttlingBackoffStrategy(FullJitterBackoffStrategy.builder().baseDelay(Duration.ofSeconds(3)).maxBackoffTime(Duration.ofSeconds(60)).build())
                        .numRetries(5)))
                .build();

        executerQueue.addFirst(ls("09b03a98-972a-45f6-a85d-e39260c8fbfc", null));
    }

    public void crawl() {

        AtomicInteger count = new AtomicInteger(0);
        RateLimiter limiter = RateLimiter.create(100.0);

        while (true) {
            try {
                ListObjectsRequest request = executerQueue.poll(30, TimeUnit.SECONDS);
                limiter.acquire();
                if (request == null) break;
                s3client.listObjects(request).handle((response, e) -> {
                    if (e != null) {
                        executerQueue.addFirst(request);
                   } else {
                        if (response.isTruncated()) {
                            if (response.nextMarker().endsWith("/")) {
                                executerQueue.addFirst(ls(response.prefix(), response.nextMarker()));
                            } else {
                                executerQueue.addFirst(ls(response.prefix(), response.nextMarker().substring(0, response.prefix().length() + Math.min(4, response.nextMarker().length() - response.prefix().length())) + "~"));
                            }
                        }
                        response.commonPrefixes().stream().map(CommonPrefix::prefix).filter(prefix -> !prefix.equals(response.marker())).forEach(prefix -> {
                            executerQueue.addFirst(ls(prefix, null));
                        });

                        if (response.contents().size() > 0
                                && response.contents().get(0).lastModified().isBefore(Instant.now().plus(-3, ChronoUnit.DAYS))
                                && response.contents().get(0).lastModified().isAfter(Instant.now().plus(-30, ChronoUnit.DAYS))
                        )
                        {
                            count.incrementAndGet();
                            System.out.println("found root:" + response.prefix() + " " + response.contents().get(0).lastModified().toString());
                        }
                    }
                    return null;
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        System.out.println("total:" + count.get());
    }

    private ListObjectsRequest ls(String prefix, String token) {
        //System.out.println("crawling prefix:" + prefix + " token:" + token);
        return ListObjectsRequest.builder().bucket(bucket).prefix(prefix).marker(token).delimiter("/").maxKeys(1000).build();
    }


    private boolean shouldContinue() {
        return true;
    }


    public static void main(String[] args) throws Exception {
        new AwesomeS3("krux-audience-segments", 5).crawl();
    }

    public String getBucket() {
        return bucket;
    }

    public int getAge() {
        return age;
    }

}
