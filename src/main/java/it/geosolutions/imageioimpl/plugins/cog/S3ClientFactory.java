/*
 *    ImageI/O-Ext - OpenSource Java Image translation Library
 *    https://github.com/quarticles/imageio-ext-cog-s3
 *    (C) 2024, Quarticle
 *
 *    Based on original work by GeoSolutions (https://github.com/geosolutions-it/imageio-ext)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    either version 3 of the License, or (at your option) any later version.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package it.geosolutions.imageioimpl.plugins.cog;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

/**
 * Factory for creating and caching S3 async clients.
 *
 * <p>Clients are cached by a combination of endpoint and region to support
 * multi-cloud deployments with different S3-compatible providers.
 *
 * <p>Cache key format: {@code endpoint::region} or {@code aws::region} for default AWS.
 */
public class S3ClientFactory {

    private static final Logger LOGGER = Logger.getLogger(S3ClientFactory.class.getName());

    private S3ClientFactory() {}

    /**
     * Cache of S3 clients keyed by endpoint+region combination.
     * This allows different clients for AWS, Wasabi, MinIO, etc.
     */
    private static final Map<String, S3AsyncClient> S3_CLIENTS = new ConcurrentHashMap<>();

    /**
     * Gets or creates an S3 async client for the given configuration.
     *
     * @param configProps the S3 configuration properties
     * @return the S3 async client
     */
    public static S3AsyncClient getS3Client(S3ConfigurationProperties configProps) {
        String cacheKey = buildCacheKey(configProps);

        return S3_CLIENTS.computeIfAbsent(cacheKey, k -> {
            LOGGER.info("Creating new S3 client for: " + cacheKey);
            return createClient(configProps);
        });
    }

    /**
     * Builds a cache key from configuration properties.
     */
    private static String buildCacheKey(S3ConfigurationProperties configProps) {
        String endpoint = configProps.getEndpoint();
        String region = configProps.getRegion();
        return (endpoint != null ? endpoint : "aws") + "::" + (region != null ? region : "default");
    }

    /**
     * Creates a new S3 async client.
     */
    private static S3AsyncClient createClient(S3ConfigurationProperties configProps) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder();

        // Configure credentials
        configureCredentials(builder, configProps);

        // Configure endpoint override for S3-compatible providers
        if (configProps.getEndpoint() != null && !configProps.getEndpoint().isEmpty()) {
            String endpoint = configProps.getEndpoint();
            // Ensure endpoint has trailing slash for proper URL construction
            if (!endpoint.endsWith("/")) {
                endpoint = endpoint + "/";
            }
            LOGGER.fine("Using custom S3 endpoint: " + endpoint);
            builder.endpointOverride(URI.create(endpoint));
        }

        // Configure region
        if (configProps.getRegion() != null && !configProps.getRegion().isEmpty()) {
            builder.region(Region.of(configProps.getRegion()));
        }

        // Configure path-style access (required for most S3-compatible providers)
        builder.forcePathStyle(configProps.getForcePathStyle());

        // Configure async executor
        builder.asyncConfiguration(b ->
                b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, createExecutor(configProps)));

        return builder.build();
    }

    /**
     * Configures credentials on the client builder.
     */
    private static void configureCredentials(S3AsyncClientBuilder builder, S3ConfigurationProperties configProps) {
        String user = configProps.getUser();
        String password = configProps.getPassword();

        if (user != null && password != null) {
            if (user.isEmpty() && password.isEmpty()) {
                // Empty credentials = anonymous access
                LOGGER.fine("Using anonymous credentials");
                builder.credentialsProvider(() -> AnonymousCredentialsProvider.create().resolveCredentials());
            } else {
                // Explicit credentials
                LOGGER.fine("Using explicit credentials for user: " + user);
                builder.credentialsProvider(() -> AwsBasicCredentials.create(user, password));
            }
        } else {
            // Use default credential chain (env vars, instance profile, etc.)
            LOGGER.fine("Using default AWS credential provider chain");
            builder.credentialsProvider(() -> DefaultCredentialsProvider.create().resolveCredentials());
        }
    }

    /**
     * Creates a thread pool executor for async operations.
     */
    private static ThreadPoolExecutor createExecutor(S3ConfigurationProperties configProps) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                configProps.getCorePoolSize(),
                configProps.getMaxPoolSize(),
                configProps.getKeepAliveTime(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10_000),
                new ThreadFactoryBuilder()
                        .threadNamePrefix("s3-async-" + configProps.getRegion() + "-")
                        .build());

        // Allow idle core threads to time out
        executor.allowCoreThreadTimeOut(true);

        return executor;
    }

    /**
     * Clears all cached S3 clients.
     * Useful for testing or reconfiguration.
     */
    public static void clearCache() {
        LOGGER.info("Clearing S3 client cache (" + S3_CLIENTS.size() + " clients)");
        S3_CLIENTS.values().forEach(client -> {
            try {
                client.close();
            } catch (Exception e) {
                LOGGER.warning("Error closing S3 client: " + e.getMessage());
            }
        });
        S3_CLIENTS.clear();
    }

    /**
     * Returns the number of cached clients.
     */
    public static int getCacheSize() {
        return S3_CLIENTS.size();
    }
}
