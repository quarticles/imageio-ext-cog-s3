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

import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for S3ConfigRegistry.
 */
class S3ConfigRegistryTest {

    @BeforeEach
    void setUp() {
        S3ConfigRegistry.clear();
    }

    @AfterEach
    void tearDown() {
        S3ConfigRegistry.clear();
    }

    @Test
    void testRegisterAndGet() {
        S3StorageConfig config = S3StorageConfig.builder()
                .endpoint("https://s3.wasabisys.com")
                .region("eu-central-1")
                .forcePathStyle(true)
                .build();

        S3ConfigRegistry.register("test-bucket", config);

        Optional<S3StorageConfig> retrieved = S3ConfigRegistry.get("test-bucket");
        assertTrue(retrieved.isPresent());
        assertEquals("https://s3.wasabisys.com", retrieved.get().getEndpoint());
        assertEquals("eu-central-1", retrieved.get().getRegion());
        assertTrue(retrieved.get().isForcePathStyle());
    }

    @Test
    void testGetNonExistent() {
        Optional<S3StorageConfig> retrieved = S3ConfigRegistry.get("non-existent");
        assertFalse(retrieved.isPresent());
    }

    @Test
    void testUnregister() {
        S3ConfigRegistry.register("test-bucket", S3StorageConfig.builder()
                .region("us-east-1")
                .build());

        assertTrue(S3ConfigRegistry.isRegistered("test-bucket"));

        S3StorageConfig removed = S3ConfigRegistry.unregister("test-bucket");
        assertNotNull(removed);
        assertFalse(S3ConfigRegistry.isRegistered("test-bucket"));
    }

    @Test
    void testDefaultConfig() {
        S3StorageConfig defaultConfig = S3StorageConfig.builder()
                .endpoint("https://default.endpoint.com")
                .region("us-west-2")
                .build();

        S3ConfigRegistry.setDefault(defaultConfig);

        // Non-existent bucket should return default
        Optional<S3StorageConfig> result = S3ConfigRegistry.getOrDefault("unknown-bucket");
        assertTrue(result.isPresent());
        assertEquals("https://default.endpoint.com", result.get().getEndpoint());
    }

    @Test
    void testBucketConfigOverridesDefault() {
        S3ConfigRegistry.setDefault(S3StorageConfig.builder()
                .endpoint("https://default.endpoint.com")
                .region("us-west-2")
                .build());

        S3ConfigRegistry.register("specific-bucket", S3StorageConfig.builder()
                .endpoint("https://specific.endpoint.com")
                .region("eu-central-1")
                .build());

        // Specific bucket should return bucket config, not default
        Optional<S3StorageConfig> result = S3ConfigRegistry.getOrDefault("specific-bucket");
        assertTrue(result.isPresent());
        assertEquals("https://specific.endpoint.com", result.get().getEndpoint());
    }

    @Test
    void testSize() {
        assertEquals(0, S3ConfigRegistry.size());

        S3ConfigRegistry.register("bucket1", S3StorageConfig.builder().region("us-east-1").build());
        assertEquals(1, S3ConfigRegistry.size());

        S3ConfigRegistry.register("bucket2", S3StorageConfig.builder().region("us-west-2").build());
        assertEquals(2, S3ConfigRegistry.size());

        S3ConfigRegistry.clear();
        assertEquals(0, S3ConfigRegistry.size());
    }

    @Test
    void testRegisterNullBucketThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                S3ConfigRegistry.register(null, S3StorageConfig.builder().build()));
    }

    @Test
    void testRegisterNullConfigThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                S3ConfigRegistry.register("bucket", null));
    }

    @Test
    void testClearAlsoRemovesDefault() {
        S3ConfigRegistry.setDefault(S3StorageConfig.builder().region("us-east-1").build());
        S3ConfigRegistry.register("bucket", S3StorageConfig.builder().region("us-west-2").build());

        S3ConfigRegistry.clear();

        assertFalse(S3ConfigRegistry.getDefault().isPresent());
        assertEquals(0, S3ConfigRegistry.size());
    }
}
