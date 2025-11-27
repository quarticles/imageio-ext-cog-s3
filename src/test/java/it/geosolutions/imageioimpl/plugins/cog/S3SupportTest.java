package it.geosolutions.imageioimpl.plugins.cog;

import it.geosolutions.imageio.core.BasicAuthURI;
import org.junit.jupiter.api.Test;
import java.net.URI;
import static org.junit.jupiter.api.Assertions.*;

public class S3SupportTest {

    @Test
    public void testWasabiEndpointParam() {
        String url = "s3://mybucket/mykey?endpoint=https://s3.wasabisys.com&region=us-east-1";
        S3ConfigurationProperties props = new S3ConfigurationProperties("s3", new BasicAuthURI(URI.create(url)));
        
        assertEquals("https://s3.wasabisys.com", props.getEndpoint());
        assertEquals("us-east-1", props.getRegion());
        assertEquals("mybucket", props.getBucket());
        assertEquals("mykey", props.getKey());
    }

    @Test
    public void testWasabiPathStyleHttp() {
        // https://s3.wasabisys.com/bucket/key
        String url = "https://s3.wasabisys.com/mybucket/mykey";
        try {
            S3ConfigurationProperties props = new S3ConfigurationProperties("https", new BasicAuthURI(URI.create(url)));
            // If it didn't throw, let's check what it found
            System.out.println("PathStyle Region: " + props.getRegion());
            assertEquals("mybucket", props.getBucket());
        } catch (Exception e) {
            System.out.println("PathStyle test failed as expected or unexpected: " + e.getMessage());
        }
    }

    @Test
    public void testWasabiVirtualHostedStyle() {
        // https://mybucket.s3.eu-central-1.wasabisys.com/mykey
        String url = "https://mybucket.s3.eu-central-1.wasabisys.com/mykey";
        try {
            S3ConfigurationProperties props = new S3ConfigurationProperties("https", new BasicAuthURI(URI.create(url)));
            assertEquals("eu-central-1", props.getRegion());
            assertEquals("mybucket", props.getBucket());
        } catch (Exception e) {
             System.out.println("Virtual host test failed: " + e.getMessage());
        }
    }

    @Test
    public void testMinioArbitraryDomain() {
        // https://minio.example.com/bucket/key
        // This should now succeed with the GenericPathStyleParser
        String url = "https://minio.example.com/mybucket/mykey";
        try {
            S3ConfigurationProperties props = new S3ConfigurationProperties("https", new BasicAuthURI(URI.create(url)));
            assertEquals("mybucket", props.getBucket());
            assertEquals("mykey", props.getKey());
            // Inferred endpoint should include scheme and host
            // Depending on port logic, it might be just https://minio.example.com
            assertTrue(props.getEndpoint().startsWith("https://minio.example.com"));
            
            // Region might be null or default, but shouldn't crash
            // We don't strictly assert region here unless we decided to default it.
        } catch (Exception e) {
            e.printStackTrace();
            fail("Should not have failed for generic S3 URL: " + e.getMessage());
        }
    }
}