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

import java.util.Objects;

/**
 * Configuration for S3-compatible storage.
 *
 * <p>Supports AWS S3, Wasabi, MinIO, DigitalOcean Spaces, and other S3-compatible providers.
 *
 * <p>Can be created via:
 * <ul>
 *   <li>Builder pattern: {@code S3StorageConfig.builder().endpoint(...).build()}</li>
 *   <li>URL query params: {@code s3://bucket/key?endpoint=https://s3.wasabisys.com&region=eu-central-1}</li>
 *   <li>Environment variables: {@code IIO_S3_AWS_ENDPOINT}, {@code IIO_S3_AWS_REGION}, etc.</li>
 *   <li>Static registry: {@code S3ConfigRegistry.register("bucket", config)}</li>
 * </ul>
 */
public class S3StorageConfig {

    private final String endpoint;
    private final String region;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final boolean forcePathStyle;
    private final int corePoolSize;
    private final int maxPoolSize;
    private final int keepAliveTime;

    private S3StorageConfig(Builder builder) {
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.accessKeyId = builder.accessKeyId;
        this.secretAccessKey = builder.secretAccessKey;
        this.forcePathStyle = builder.forcePathStyle;
        this.corePoolSize = builder.corePoolSize;
        this.maxPoolSize = builder.maxPoolSize;
        this.keepAliveTime = builder.keepAliveTime;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public boolean isForcePathStyle() {
        return forcePathStyle;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getKeepAliveTime() {
        return keepAliveTime;
    }

    /**
     * Returns a unique key for caching S3 clients.
     * Different endpoints/regions should use different clients.
     */
    public String getCacheKey() {
        return (endpoint != null ? endpoint : "aws") + "::" + (region != null ? region : "default");
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3StorageConfig that = (S3StorageConfig) o;
        return forcePathStyle == that.forcePathStyle
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(region, that.region)
                && Objects.equals(accessKeyId, that.accessKeyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, region, accessKeyId, forcePathStyle);
    }

    @Override
    public String toString() {
        return "S3StorageConfig{"
                + "endpoint='" + endpoint + '\''
                + ", region='" + region + '\''
                + ", forcePathStyle=" + forcePathStyle
                + ", hasCredentials=" + (accessKeyId != null)
                + '}';
    }

    public static class Builder {
        private String endpoint;
        private String region;
        private String accessKeyId;
        private String secretAccessKey;
        private boolean forcePathStyle = false;
        private int corePoolSize = 50;
        private int maxPoolSize = 128;
        private int keepAliveTime = 10;

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder accessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        public Builder secretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        public Builder credentials(String accessKeyId, String secretAccessKey) {
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        public Builder forcePathStyle(boolean forcePathStyle) {
            this.forcePathStyle = forcePathStyle;
            return this;
        }

        public Builder corePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        public Builder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder keepAliveTime(int keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        public S3StorageConfig build() {
            return new S3StorageConfig(this);
        }
    }
}
