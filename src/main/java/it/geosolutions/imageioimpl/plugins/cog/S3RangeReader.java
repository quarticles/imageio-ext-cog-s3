/*
 *    ImageI/O-Ext - OpenSource Java Image translation Library
 *    https://github.com/quarticles/imageio-ext-cog-s3
 *    (C) 2024, Quarticle
 *
 *    Based on original work by GeoSolutions (https://github.com/geosolutions-it/imageio-ext)
 *    Original author: joshfix (2019-08-21)
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

import it.geosolutions.imageio.core.BasicAuthURI;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * S3 Range Reader with support for S3-compatible storage providers.
 *
 * <p>Reads URIs from S3 with the following formats:
 * <ul>
 *   <li>{@code https://s3-<region>.amazonaws.com/<bucket>/<key>}</li>
 *   <li>{@code https://s3.<region>.amazonaws.com/<bucket>/<key>}</li>
 *   <li>{@code https://<bucket>.s3-<region>.amazonaws.com/<key>}</li>
 *   <li>{@code https://<bucket>.s3.<region>.amazonaws.com/<key>}</li>
 *   <li>{@code s3://<bucket>/<key>?region=...&endpoint=...}</li>
 * </ul>
 *
 * <p>This fork adds support for S3-compatible providers (Wasabi, MinIO, DigitalOcean Spaces, etc.)
 * by using the configured endpoint instead of hardcoded ".amazonaws.com".
 *
 * <p>Configuration sources (in priority order):
 * <ol>
 *   <li>URL query parameters: {@code s3://bucket/key?endpoint=...&region=...}</li>
 *   <li>BasicAuthURI credentials: {@code s3://user:pass@bucket/key}</li>
 *   <li>Bucket-specific registry: {@link S3ConfigRegistry}</li>
 *   <li>Environment variables: {@code IIO_S3_AWS_ENDPOINT}, {@code IIO_S3_AWS_REGION}, etc.</li>
 * </ol>
 *
 * <p>API documentation: https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/aws-sdk-java-dg-v2.pdf
 *
 * @author joshfix Created on 2019-08-21
 * @author Quarticle - Fork with S3-compatible endpoint support
 */
public class S3RangeReader extends AbstractRangeReader {

    protected S3Client client;
    protected S3ConfigurationProperties configProps;

    private static final Logger LOGGER = Logger.getLogger(S3RangeReader.class.getName());

    /** Shared executor for parallel range reads */
    private static final ExecutorService RANGE_READ_EXECUTOR = createRangeReadExecutor();

    /** Maximum number of parallel range reads per request */
    private static final int MAX_PARALLEL_READS = Integer.parseInt(
            System.getProperty("cog.s3.maxParallelReads", "8"));

    private static ExecutorService createRangeReadExecutor() {
        int poolSize = Integer.parseInt(System.getProperty("cog.s3.rangeReadPoolSize", "32"));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                poolSize / 2,
                poolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> {
                    Thread t = new Thread(r, "cog-s3-range-reader");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    public S3RangeReader(String url, int headerLength) {
        this(URI.create(url), headerLength);
    }

    public S3RangeReader(URL url, int headerLength) {
        this(URI.create(url.toString()), headerLength);
    }

    public S3RangeReader(URI uri, int headerLength) {
        this(new BasicAuthURI(uri), headerLength);
    }

    public S3RangeReader(BasicAuthURI uri, int headerLength) {
        super(uri, headerLength);
        LOGGER.fine(() -> "Creating S3RangeReader for URI: " + uri.getUri());
        try {
            configProps = new S3ConfigurationProperties(uri.getUri().getScheme(), uri);
            LOGGER.fine(() -> "S3ConfigurationProperties created: bucket=" + configProps.getBucket()
                    + ", key=" + configProps.getKey()
                    + ", endpoint=" + configProps.getEndpoint()
                    + ", region=" + configProps.getRegion()
                    + ", forcePathStyle=" + configProps.getForcePathStyle());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create S3ConfigurationProperties for URI " + uri.getUri(), e);
            throw e;
        }
        try {
            client = S3ClientFactory.getS3Client(configProps);
            LOGGER.fine(() -> "S3Client obtained successfully");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create S3Client", e);
            throw e;
        }
    }

    @Override
    public byte[] fetchHeader() {
        byte[] currentHeader = data.get(0L);
        if (currentHeader != null) {
            headerOffset = currentHeader.length;
        }
        GetObjectRequest headerRequest = buildRequest();
        try {
            ResponseBytes<GetObjectResponse> responseBytes =
                    client.getObject(headerRequest, ResponseTransformer.toBytes());

            // get the header bytes
            byte[] headerBytes = responseBytes.asByteArray();
            if (headerOffset != 0) {
                byte[] oldHeader = data.get(0L);
                byte[] newHeader = new byte[headerBytes.length + oldHeader.length];
                System.arraycopy(oldHeader, 0, newHeader, 0, oldHeader.length);
                System.arraycopy(headerBytes, 0, newHeader, oldHeader.length, headerBytes.length);
                headerBytes = newHeader;
            }

            data.put(0L, headerBytes);
            return headerBytes;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error reading header for " + uri, e);
            throw new S3ReadException("Error reading header for " + uri, e);
        }
    }

    @Override
    public Map<Long, byte[]> read(Collection<long[]> ranges) {
        return read(ranges.toArray(new long[][] {}));
    }

    @Override
    public byte[] readHeader() {
        LOGGER.fine("reading header");
        byte[] currentHeader = HEADERS_CACHE.get(uri.toString());

        if (currentHeader != null) {
            return currentHeader;
        }
        GetObjectRequest headerRequest = buildRequest();
        try {
            ResponseBytes<GetObjectResponse> responseBytes =
                    client.getObject(headerRequest, ResponseTransformer.toBytes());

            // get the header bytes
            byte[] headerBytes = responseBytes.asByteArray();
            data.put(0L, headerBytes);
            HEADERS_CACHE.put(uri.toString(), headerBytes);
            return headerBytes;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error reading header for " + uri, e);
            throw new S3ReadException("Error reading header for " + uri, e);
        }
    }

    private GetObjectRequest buildRequest() {
        return GetObjectRequest.builder()
                .bucket(configProps.getBucket())
                .key(configProps.getKey())
                .range("bytes=" + headerOffset + "-" + (headerOffset + headerLength - 1))
                .build();
    }

    @Override
    public Map<Long, byte[]> read(long[]... ranges) {
        final long[][] reconciledRanges = reconcileRanges(ranges);

        Instant start = Instant.now();
        Map<Long, byte[]> values = new ConcurrentHashMap<>();

        // Collect ranges that need to be fetched
        List<long[]> missingRanges = new ArrayList<>();
        for (long[] range : reconciledRanges) {
            final long rangeStart = range[0];
            byte[] dataRange = data.get(rangeStart);
            if (dataRange != null) {
                values.put(rangeStart, dataRange);
            } else {
                missingRanges.add(range);
            }
        }

        // Fetch missing ranges in parallel
        if (!missingRanges.isEmpty()) {
            if (missingRanges.size() == 1) {
                // Single range - read directly without executor overhead
                long[] range = missingRanges.get(0);
                byte[] fetchedData = readRange(range[0], range[1]);
                values.put(range[0], fetchedData);
                data.put(range[0], fetchedData);
            } else {
                // Multiple ranges - read in parallel
                readRangesParallel(missingRanges, values);
            }
        }

        Instant end = Instant.now();
        final int totalRanges = reconciledRanges.length;
        final int fetchedCount = missingRanges.size();
        LOGGER.fine(() -> "Time to read " + totalRanges + " ranges (" + fetchedCount
                + " fetched): " + Duration.between(start, end).toMillis() + "ms");
        return values;
    }

    /**
     * Reads multiple ranges in parallel using the shared executor.
     *
     * @param ranges the ranges to read
     * @param values the map to store results
     */
    private void readRangesParallel(List<long[]> ranges, Map<Long, byte[]> values) {
        // Limit parallelism to avoid overwhelming the connection pool
        int batchSize = Math.min(ranges.size(), MAX_PARALLEL_READS);

        List<CompletableFuture<Void>> futures = new ArrayList<>(batchSize);

        for (long[] range : ranges) {
            final long rangeStart = range[0];
            final long rangeEnd = range[1];

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    byte[] fetchedData = readRange(rangeStart, rangeEnd);
                    values.put(rangeStart, fetchedData);
                    data.put(rangeStart, fetchedData);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Error reading range " + rangeStart + "-" + rangeEnd, e);
                    throw new CompletionException(e);
                }
            }, RANGE_READ_EXECUTOR);

            futures.add(future);
        }

        // Wait for all futures to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new S3ReadException("Interrupted while reading ranges from " + uri, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof S3ReadException) {
                throw (S3ReadException) cause;
            }
            throw new S3ReadException("Error reading ranges from " + uri, cause);
        } catch (TimeoutException e) {
            throw new S3ReadException("Timeout reading ranges from " + uri, e);
        }
    }

    /**
     * Reads a single range from S3 synchronously.
     *
     * @param rangeStart the start byte offset
     * @param rangeEnd the end byte offset
     * @return the byte array containing the range data
     */
    byte[] readRange(final long rangeStart, long rangeEnd) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(configProps.getBucket())
                .key(configProps.getKey())
                .range("bytes=" + rangeStart + "-" + rangeEnd)
                .build();
        try {
            ResponseBytes<GetObjectResponse> responseBytes =
                    client.getObject(request, ResponseTransformer.toBytes());
            return responseBytes.asByteArray();
        } catch (Exception e) {
            throw new S3ReadException("Error reading range " + rangeStart + "-" + rangeEnd
                    + " from " + configProps.getBucket() + "/" + configProps.getKey(), e);
        }
    }

    /**
     * Returns the URL for this S3 resource.
     *
     * <p>This method supports S3-compatible storage providers by using the configured
     * endpoint instead of hardcoding ".amazonaws.com". This enables support for:
     * <ul>
     *   <li>Wasabi (s3.wasabisys.com)</li>
     *   <li>MinIO</li>
     *   <li>DigitalOcean Spaces</li>
     *   <li>Other S3-compatible providers</li>
     * </ul>
     *
     * <p>If no custom endpoint is configured, falls back to the standard AWS S3 URL format.
     *
     * @return the URL for this S3 resource
     * @throws MalformedURLException if the URL cannot be constructed
     */
    @Override
    public URL getURL() throws MalformedURLException {
        String scheme = uri.getScheme().toLowerCase();
        if (scheme.startsWith("s3")) {
            String endpoint = configProps.getEndpoint();
            if (endpoint != null && !endpoint.isEmpty()) {
                // Use configured endpoint for S3-compatible providers
                return buildCustomEndpointURL(endpoint);
            } else {
                // No custom endpoint - use default AWS S3 URL format
                return new URL("https://" + configProps.getBucket() + ".s3." + configProps.getRegion()
                        + ".amazonaws.com/" + configProps.getKey());
            }
        }
        return super.getURL();
    }

    /**
     * Builds a URL using the configured custom endpoint.
     *
     * @param endpoint the S3-compatible endpoint URL
     * @return the URL for this S3 resource
     * @throws MalformedURLException if the URL cannot be constructed
     */
    private URL buildCustomEndpointURL(String endpoint) throws MalformedURLException {
        // Remove trailing slash if present
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }

        String urlStr;
        if (configProps.getForcePathStyle()) {
            // Path-style: https://s3.wasabisys.com/bucket/key
            urlStr = endpoint + "/" + configProps.getBucket() + "/" + configProps.getKey();
        } else {
            // Virtual-hosted-style: https://bucket.s3.region.wasabisys.com/key
            try {
                URI endpointUri = URI.create(endpoint);
                String endpointHost = endpointUri.getHost();
                String endpointScheme = endpointUri.getScheme();
                if (endpointScheme == null) {
                    endpointScheme = "https";
                }
                urlStr = endpointScheme + "://" + configProps.getBucket() + "." + endpointHost + "/"
                        + configProps.getKey();
            } catch (Exception e) {
                // Fallback to path-style if endpoint parsing fails
                LOGGER.warning("Failed to parse endpoint URI, using path-style: " + e.getMessage());
                urlStr = endpoint + "/" + configProps.getBucket() + "/" + configProps.getKey();
            }
        }
        LOGGER.fine("Generated S3-compatible URL: " + urlStr);
        return new URL(urlStr);
    }
}
