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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Registry for per-bucket S3 storage configurations.
 *
 * <p>Allows registering different S3 configurations for different buckets,
 * enabling multi-cloud and multi-region setups.
 *
 * <p>Configuration lookup priority:
 * <ol>
 *   <li>URL query parameters (inline config)</li>
 *   <li>Bucket-specific registry entry</li>
 *   <li>Global environment variables (IIO_S3_AWS_*)</li>
 *   <li>Default AWS SDK credential chain</li>
 * </ol>
 *
 * <p>Example usage:
 * <pre>
 * // Register Wasabi config for specific bucket
 * S3ConfigRegistry.register("my-wasabi-bucket",
 *     S3StorageConfig.builder()
 *         .endpoint("https://s3.eu-central-1.wasabisys.com")
 *         .region("eu-central-1")
 *         .credentials("access-key", "secret-key")
 *         .forcePathStyle(true)
 *         .build());
 *
 * // Register MinIO config
 * S3ConfigRegistry.register("my-minio-bucket",
 *     S3StorageConfig.builder()
 *         .endpoint("http://minio.local:9000")
 *         .region("us-east-1")
 *         .credentials("minio-user", "minio-pass")
 *         .forcePathStyle(true)
 *         .build());
 * </pre>
 */
public final class S3ConfigRegistry {

    private static final Logger LOGGER = Logger.getLogger(S3ConfigRegistry.class.getName());

    private static final Map<String, S3StorageConfig> BUCKET_CONFIGS = new ConcurrentHashMap<>();
    private static volatile S3StorageConfig defaultConfig = null;

    private S3ConfigRegistry() {
        // Utility class
    }

    /**
     * Registers a configuration for a specific bucket.
     *
     * @param bucketName the bucket name
     * @param config     the S3 storage configuration
     */
    public static void register(String bucketName, S3StorageConfig config) {
        if (bucketName == null || bucketName.isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }
        LOGGER.info("Registering S3 config for bucket: " + bucketName + " -> " + config);
        BUCKET_CONFIGS.put(bucketName, config);
    }

    /**
     * Unregisters the configuration for a specific bucket.
     *
     * @param bucketName the bucket name
     * @return the removed configuration, or null if none was registered
     */
    public static S3StorageConfig unregister(String bucketName) {
        LOGGER.info("Unregistering S3 config for bucket: " + bucketName);
        return BUCKET_CONFIGS.remove(bucketName);
    }

    /**
     * Gets the configuration for a specific bucket.
     *
     * @param bucketName the bucket name
     * @return the configuration, or empty if not registered
     */
    public static Optional<S3StorageConfig> get(String bucketName) {
        return Optional.ofNullable(BUCKET_CONFIGS.get(bucketName));
    }

    /**
     * Sets the default configuration used when no bucket-specific config is found.
     *
     * @param config the default configuration
     */
    public static void setDefault(S3StorageConfig config) {
        LOGGER.info("Setting default S3 config: " + config);
        defaultConfig = config;
    }

    /**
     * Gets the default configuration.
     *
     * @return the default configuration, or empty if not set
     */
    public static Optional<S3StorageConfig> getDefault() {
        return Optional.ofNullable(defaultConfig);
    }

    /**
     * Gets the configuration for a bucket, falling back to default if not found.
     *
     * @param bucketName the bucket name
     * @return the configuration, or empty if neither bucket-specific nor default is set
     */
    public static Optional<S3StorageConfig> getOrDefault(String bucketName) {
        S3StorageConfig config = BUCKET_CONFIGS.get(bucketName);
        if (config != null) {
            return Optional.of(config);
        }
        return Optional.ofNullable(defaultConfig);
    }

    /**
     * Clears all registered configurations.
     */
    public static void clear() {
        LOGGER.info("Clearing all S3 configs");
        BUCKET_CONFIGS.clear();
        defaultConfig = null;
    }

    /**
     * Returns the number of registered bucket configurations.
     */
    public static int size() {
        return BUCKET_CONFIGS.size();
    }

    /**
     * Checks if a configuration is registered for a specific bucket.
     */
    public static boolean isRegistered(String bucketName) {
        return BUCKET_CONFIGS.containsKey(bucketName);
    }
}
