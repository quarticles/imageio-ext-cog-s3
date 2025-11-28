/*
 *    ImageI/O-Ext - OpenSource Java Image translation Library
 *    https://github.com/quarticles/imageio-ext-cog-s3
 *    (C) 2024, Quarticle
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    either version 3 of the License, or (at your option) any later version.
 */
package it.geosolutions.imageioimpl.plugins.cog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for S3StorageConfig builder and value object.
 */
class S3StorageConfigTest {

    @Test
    void testBuilderDefaults() {
        S3StorageConfig config = S3StorageConfig.builder().build();

        assertNull(config.getEndpoint());
        assertNull(config.getRegion());
        assertNull(config.getAccessKeyId());
        assertNull(config.getSecretAccessKey());
        assertFalse(config.isForcePathStyle());
        assertEquals(50, config.getCorePoolSize());
        assertEquals(128, config.getMaxPoolSize());
        assertEquals(10, config.getKeepAliveTime());
    }

    @Test
    void testBuilderWithAllValues() {
        S3StorageConfig config = S3StorageConfig.builder()
                .endpoint("https://s3.wasabisys.com")
                .region("eu-central-1")
                .accessKeyId("myAccessKey")
                .secretAccessKey("mySecretKey")
                .forcePathStyle(true)
                .corePoolSize(10)
                .maxPoolSize(50)
                .keepAliveTime(30)
                .build();

        assertEquals("https://s3.wasabisys.com", config.getEndpoint());
        assertEquals("eu-central-1", config.getRegion());
        assertEquals("myAccessKey", config.getAccessKeyId());
        assertEquals("mySecretKey", config.getSecretAccessKey());
        assertTrue(config.isForcePathStyle());
        assertEquals(10, config.getCorePoolSize());
        assertEquals(50, config.getMaxPoolSize());
        assertEquals(30, config.getKeepAliveTime());
    }

    @Test
    void testCredentialsMethod() {
        S3StorageConfig config = S3StorageConfig.builder()
                .credentials("accessKey", "secretKey")
                .build();

        assertEquals("accessKey", config.getAccessKeyId());
        assertEquals("secretKey", config.getSecretAccessKey());
    }

    @Test
    void testCacheKey() {
        S3StorageConfig config1 = S3StorageConfig.builder()
                .endpoint("https://s3.wasabisys.com")
                .region("eu-central-1")
                .build();

        assertEquals("https://s3.wasabisys.com::eu-central-1", config1.getCacheKey());

        S3StorageConfig config2 = S3StorageConfig.builder()
                .region("us-west-2")
                .build();

        assertEquals("aws::us-west-2", config2.getCacheKey());

        S3StorageConfig config3 = S3StorageConfig.builder().build();

        assertEquals("aws::default", config3.getCacheKey());
    }

    @Test
    void testEquality() {
        S3StorageConfig config1 = S3StorageConfig.builder()
                .endpoint("https://endpoint.com")
                .region("eu-central-1")
                .accessKeyId("key")
                .forcePathStyle(true)
                .build();

        S3StorageConfig config2 = S3StorageConfig.builder()
                .endpoint("https://endpoint.com")
                .region("eu-central-1")
                .accessKeyId("key")
                .forcePathStyle(true)
                .build();

        S3StorageConfig config3 = S3StorageConfig.builder()
                .endpoint("https://different.com")
                .region("eu-central-1")
                .accessKeyId("key")
                .forcePathStyle(true)
                .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1, config3);
    }

    @Test
    void testToString() {
        S3StorageConfig config = S3StorageConfig.builder()
                .endpoint("https://s3.wasabisys.com")
                .region("eu-central-1")
                .accessKeyId("myKey")
                .forcePathStyle(true)
                .build();

        String str = config.toString();
        assertTrue(str.contains("endpoint='https://s3.wasabisys.com'"));
        assertTrue(str.contains("region='eu-central-1'"));
        assertTrue(str.contains("forcePathStyle=true"));
        assertTrue(str.contains("hasCredentials=true"));
        // Should not expose actual credentials
        assertFalse(str.contains("myKey"));
    }
}
