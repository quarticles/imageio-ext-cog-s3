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
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Factory for creating and caching S3 clients with resource management.
 *
 * <p>Clients are cached by a combination of endpoint and region to support
 * multi-cloud deployments with different S3-compatible providers.
 *
 * <p>Features:
 * <ul>
 *   <li>Bounded cache with configurable max size</li>
 *   <li>Time-based eviction of unused clients</li>
 *   <li>Configurable connection pooling</li>
 *   <li>Automatic resource cleanup</li>
 * </ul>
 *
 * <p>Cache key format: {@code endpoint::region} or {@code aws::region} for default AWS.
 *
 * <p>Configuration via system properties:
 * <ul>
 *   <li>{@code cog.s3.cache.maxSize} - Maximum cached clients (default: 16)</li>
 *   <li>{@code cog.s3.cache.ttlMinutes} - Client TTL in minutes (default: 30)</li>
 *   <li>{@code cog.s3.http.maxConnections} - Max HTTP connections per client (default: 50)</li>
 *   <li>{@code cog.s3.http.connectionTimeoutSeconds} - Connection timeout (default: 10)</li>
 *   <li>{@code cog.s3.http.socketTimeoutSeconds} - Socket timeout (default: 30)</li>
 * </ul>
 */
public class S3ClientFactory {

    private static final Logger LOGGER = Logger.getLogger(S3ClientFactory.class.getName());

    // Configuration
    private static final int MAX_CACHE_SIZE = Integer.parseInt(
            System.getProperty("cog.s3.cache.maxSize", "16"));
    private static final long CACHE_TTL_MINUTES = Long.parseLong(
            System.getProperty("cog.s3.cache.ttlMinutes", "30"));
    private static final int MAX_CONNECTIONS = Integer.parseInt(
            System.getProperty("cog.s3.http.maxConnections", "50"));
    private static final int CONNECTION_TIMEOUT_SECONDS = Integer.parseInt(
            System.getProperty("cog.s3.http.connectionTimeoutSeconds", "10"));
    private static final int SOCKET_TIMEOUT_SECONDS = Integer.parseInt(
            System.getProperty("cog.s3.http.socketTimeoutSeconds", "30"));

    private S3ClientFactory() {}

    /**
     * Cache entry with last access time for TTL-based eviction.
     */
    private static class CacheEntry {
        final S3Client client;
        volatile long lastAccessTime;

        CacheEntry(S3Client client) {
            this.client = client;
            this.lastAccessTime = System.currentTimeMillis();
        }

        void touch() {
            this.lastAccessTime = System.currentTimeMillis();
        }

        boolean isExpired(long ttlMillis) {
            return System.currentTimeMillis() - lastAccessTime > ttlMillis;
        }
    }

    /**
     * Cache of S3 clients keyed by endpoint+region combination.
     * This allows different clients for AWS, Wasabi, MinIO, etc.
     */
    private static final Map<String, CacheEntry> S3_CLIENTS = new ConcurrentHashMap<>();

    /**
     * Scheduler for periodic cache cleanup.
     */
    private static final ScheduledExecutorService CLEANUP_SCHEDULER;

    static {
        // Start cleanup scheduler
        CLEANUP_SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "s3-client-cache-cleanup");
            t.setDaemon(true);
            return t;
        });
        CLEANUP_SCHEDULER.scheduleAtFixedRate(
                S3ClientFactory::cleanupExpiredClients,
                CACHE_TTL_MINUTES,
                CACHE_TTL_MINUTES / 2,
                TimeUnit.MINUTES);

        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            CLEANUP_SCHEDULER.shutdownNow();
            clearCache();
        }, "s3-client-shutdown"));
    }

    /**
     * Gets or creates an S3 client for the given configuration.
     *
     * @param configProps the S3 configuration properties
     * @return the S3 client
     */
    public static S3Client getS3Client(S3ConfigurationProperties configProps) {
        String cacheKey = buildCacheKey(configProps);

        CacheEntry entry = S3_CLIENTS.compute(cacheKey, (key, existing) -> {
            if (existing != null && !existing.isExpired(CACHE_TTL_MINUTES * 60 * 1000)) {
                existing.touch();
                return existing;
            }

            // Close existing expired client
            if (existing != null) {
                closeClientSafely(existing.client, key);
            }

            // Check cache size and evict if needed
            evictIfNeeded();

            LOGGER.info("Creating new S3 client for: " + key);
            return new CacheEntry(createClient(configProps));
        });

        return entry.client;
    }

    /**
     * Evicts oldest entries if cache is at max size.
     */
    private static void evictIfNeeded() {
        while (S3_CLIENTS.size() >= MAX_CACHE_SIZE) {
            // Find oldest entry
            String oldestKey = null;
            long oldestTime = Long.MAX_VALUE;

            for (Map.Entry<String, CacheEntry> entry : S3_CLIENTS.entrySet()) {
                if (entry.getValue().lastAccessTime < oldestTime) {
                    oldestTime = entry.getValue().lastAccessTime;
                    oldestKey = entry.getKey();
                }
            }

            if (oldestKey != null) {
                CacheEntry removed = S3_CLIENTS.remove(oldestKey);
                if (removed != null) {
                    LOGGER.fine("Evicting S3 client from cache: " + oldestKey);
                    closeClientSafely(removed.client, oldestKey);
                }
            } else {
                break;
            }
        }
    }

    /**
     * Removes expired clients from cache.
     */
    private static void cleanupExpiredClients() {
        long ttlMillis = CACHE_TTL_MINUTES * 60 * 1000;
        int removed = 0;

        Iterator<Map.Entry<String, CacheEntry>> it = S3_CLIENTS.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CacheEntry> entry = it.next();
            if (entry.getValue().isExpired(ttlMillis)) {
                it.remove();
                closeClientSafely(entry.getValue().client, entry.getKey());
                removed++;
            }
        }

        if (removed > 0) {
            LOGGER.fine("Cleaned up " + removed + " expired S3 clients");
        }
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
     * Creates a new S3 client with configured HTTP client.
     */
    private static S3Client createClient(S3ConfigurationProperties configProps) {
        S3ClientBuilder builder = S3Client.builder();

        // Configure HTTP client with connection pooling
        builder.httpClientBuilder(ApacheHttpClient.builder()
                .maxConnections(MAX_CONNECTIONS)
                .connectionTimeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS))
                .socketTimeout(Duration.ofSeconds(SOCKET_TIMEOUT_SECONDS))
                .connectionAcquisitionTimeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS * 2)));

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

        return builder.build();
    }

    /**
     * Configures credentials on the client builder.
     */
    private static void configureCredentials(S3ClientBuilder builder, S3ConfigurationProperties configProps) {
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
     * Safely closes an S3 client, logging any errors.
     */
    private static void closeClientSafely(S3Client client, String key) {
        try {
            client.close();
            LOGGER.fine("Closed S3 client: " + key);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error closing S3 client: " + key, e);
        }
    }

    /**
     * Clears all cached S3 clients.
     * Useful for testing or reconfiguration.
     */
    public static void clearCache() {
        LOGGER.info("Clearing S3 client cache (" + S3_CLIENTS.size() + " clients)");
        S3_CLIENTS.forEach((key, entry) -> closeClientSafely(entry.client, key));
        S3_CLIENTS.clear();
    }

    /**
     * Returns the number of cached clients.
     */
    public static int getCacheSize() {
        return S3_CLIENTS.size();
    }

    /**
     * Returns cache statistics for monitoring.
     */
    public static String getCacheStats() {
        long ttlMillis = CACHE_TTL_MINUTES * 60 * 1000;
        long now = System.currentTimeMillis();
        int active = 0;
        int expiring = 0;

        for (CacheEntry entry : S3_CLIENTS.values()) {
            long age = now - entry.lastAccessTime;
            if (age < ttlMillis / 2) {
                active++;
            } else {
                expiring++;
            }
        }

        return String.format("S3ClientCache{size=%d, active=%d, expiring=%d, maxSize=%d, ttlMinutes=%d}",
                S3_CLIENTS.size(), active, expiring, MAX_CACHE_SIZE, CACHE_TTL_MINUTES);
    }
}
