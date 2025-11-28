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

import it.geosolutions.imageio.core.BasicAuthURI;
import java.net.URI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for S3ConfigurationProperties URL parsing.
 */
class S3ConfigurationPropertiesTest {

    @BeforeEach
    void setUp() {
        // Clear registry before each test
        S3ConfigRegistry.clear();
    }

    @AfterEach
    void tearDown() {
        S3ConfigRegistry.clear();
    }

    @Test
    void testS3SchemeWithQueryParams() {
        // s3://bucket/key?region=eu-central-1&endpoint=https://s3.wasabisys.com
        URI uri = URI.create("s3://my-bucket/path/to/file.tif?region=eu-central-1&endpoint=https://s3.wasabisys.com");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("S3", basicAuthURI);

        assertEquals("my-bucket", props.getBucket());
        assertEquals("path/to/file.tif", props.getKey());
        assertEquals("eu-central-1", props.getRegion());
        assertEquals("https://s3.wasabisys.com", props.getEndpoint());
    }

    @Test
    void testS3SchemeWithPathStyleParam() {
        URI uri = URI.create("s3://my-bucket/path/to/file.tif?region=us-east-1&pathstyle=true");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("S3", basicAuthURI);

        assertEquals("my-bucket", props.getBucket());
        assertEquals("path/to/file.tif", props.getKey());
        assertEquals("us-east-1", props.getRegion());
        assertTrue(props.getForcePathStyle());
    }

    @Test
    void testHttpsPathStyleAWS() {
        // https://s3.us-west-2.amazonaws.com/bucket/key
        URI uri = URI.create("https://s3.us-west-2.amazonaws.com/my-bucket/path/to/file.tif");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("HTTPS", basicAuthURI);

        assertEquals("my-bucket", props.getBucket());
        assertEquals("path/to/file.tif", props.getKey());
        assertEquals("us-west-2", props.getRegion());
    }

    @Test
    void testHttpsVirtualHostedStyleAWS() {
        // https://bucket.s3.us-west-2.amazonaws.com/key
        URI uri = URI.create("https://my-bucket.s3.us-west-2.amazonaws.com/path/to/file.tif");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("HTTPS", basicAuthURI);

        assertEquals("my-bucket", props.getBucket());
        assertEquals("path/to/file.tif", props.getKey());
        assertEquals("us-west-2", props.getRegion());
    }

    @Test
    void testRegistryConfigTakesPrecedence() {
        // Register config for bucket
        S3ConfigRegistry.register("my-bucket", S3StorageConfig.builder()
                .endpoint("https://s3.eu-central-1.wasabisys.com")
                .region("eu-central-1")
                .forcePathStyle(true)
                .build());

        // URL without explicit endpoint/region
        URI uri = URI.create("s3://my-bucket/path/to/file.tif");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("S3", basicAuthURI);

        assertEquals("my-bucket", props.getBucket());
        assertEquals("https://s3.eu-central-1.wasabisys.com", props.getEndpoint());
        assertEquals("eu-central-1", props.getRegion());
        assertTrue(props.getForcePathStyle());
    }

    @Test
    void testUrlParamsOverrideRegistry() {
        // Register config for bucket
        S3ConfigRegistry.register("my-bucket", S3StorageConfig.builder()
                .endpoint("https://s3.eu-central-1.wasabisys.com")
                .region("eu-central-1")
                .build());

        // URL with explicit endpoint (should override registry)
        URI uri = URI.create("s3://my-bucket/path/to/file.tif?endpoint=https://minio.local:9000&region=us-east-1");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("S3", basicAuthURI);

        assertEquals("https://minio.local:9000", props.getEndpoint());
        assertEquals("us-east-1", props.getRegion());
    }

    @Test
    void testFilenameExtraction() {
        URI uri = URI.create("s3://my-bucket/some/deep/path/myfile.tif?region=us-east-1");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        S3ConfigurationProperties props = new S3ConfigurationProperties("S3", basicAuthURI);

        assertEquals("myfile.tif", props.getFilename());
    }

    @Test
    void testGenericPathStyleEndpoint() {
        // Generic S3-compatible endpoint (MinIO, etc.)
        URI uri = URI.create("https://minio.local:9000/my-bucket/path/to/file.tif");
        BasicAuthURI basicAuthURI = new BasicAuthURI(uri);

        // Need to set region via env or registry for generic endpoints
        S3ConfigRegistry.setDefault(S3StorageConfig.builder()
                .region("us-east-1")
                .forcePathStyle(true)
                .build());

        S3ConfigurationProperties props = new S3ConfigurationProperties("HTTPS", basicAuthURI);

        assertEquals("my-bucket", props.getBucket());
        assertEquals("path/to/file.tif", props.getKey());
        assertEquals("https://minio.local:9000", props.getEndpoint());
    }
}
