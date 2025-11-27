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

import it.geosolutions.imageio.core.BasicAuthURI;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

/**
 * Configuration properties for S3 connections.
 *
 * <p>Supports multiple configuration sources (in priority order):
 * <ol>
 *   <li>URL query parameters: {@code s3://bucket/key?endpoint=...&region=...}</li>
 *   <li>BasicAuthURI credentials: {@code s3://user:pass@bucket/key}</li>
 *   <li>Bucket-specific registry: {@link S3ConfigRegistry}</li>
 *   <li>Environment variables: {@code IIO_S3_AWS_*}</li>
 *   <li>System properties</li>
 * </ol>
 *
 * <p>Supported URL query parameters:
 * <ul>
 *   <li>{@code endpoint} - S3 endpoint URL (e.g., https://s3.wasabisys.com)</li>
 *   <li>{@code region} - AWS region (e.g., eu-central-1)</li>
 *   <li>{@code pathStyle} - Use path-style access (true/false)</li>
 * </ul>
 */
public class S3ConfigurationProperties {

    private static final Logger LOGGER = Logger.getLogger(S3ConfigurationProperties.class.getName());

    // URL parsing constants
    private static final String S3_DOT = "s3.";
    private static final String S3_DASH = "s3-";
    static final String S3_DOT_VH = "." + S3_DOT;
    private static final String S3_DASH_VH = "." + S3_DASH;
    static final String AMAZON_AWS = ".amazonaws.com";

    // Configuration values
    private String user;
    private String password;
    private String endpoint;
    private String region;
    private String alias;
    private String bucket;
    private String key;
    private String filename;
    private int corePoolSize;
    private int maxPoolSize;
    private int keepAliveTime;
    private boolean forcePathStyle;

    // Environment variable keys
    public final String AWS_S3_USER_KEY;
    public final String AWS_S3_PASSWORD_KEY;
    public final String AWS_S3_ENDPOINT_KEY;
    public final String AWS_S3_REGION_KEY;
    public final String AWS_S3_CORE_POOL_SIZE_KEY;
    public final String AWS_S3_MAX_POOL_SIZE_KEY;
    public final String AWS_S3_KEEP_ALIVE_TIME;
    public final String AWS_S3_FORCE_PATH_STYLE;

    /**
     * Creates configuration from a BasicAuthURI.
     *
     * @param alias  the scheme alias (e.g., "S3", "HTTP")
     * @param cogUri the URI with optional embedded credentials
     */
    public S3ConfigurationProperties(String alias, BasicAuthURI cogUri) {
        this.alias = alias.toUpperCase();

        // Build environment variable keys based on alias
        AWS_S3_USER_KEY = "IIO_" + this.alias + "_AWS_USER";
        AWS_S3_PASSWORD_KEY = "IIO_" + this.alias + "_AWS_PASSWORD";
        AWS_S3_ENDPOINT_KEY = "IIO_" + this.alias + "_AWS_ENDPOINT";
        AWS_S3_REGION_KEY = "IIO_" + this.alias + "_AWS_REGION";
        AWS_S3_CORE_POOL_SIZE_KEY = "IIO_" + this.alias + "_AWS_CORE_POOL_SIZE";
        AWS_S3_MAX_POOL_SIZE_KEY = "IIO_" + this.alias + "_AWS_MAX_POOL_SIZE";
        AWS_S3_KEEP_ALIVE_TIME = "IIO_" + this.alias + "_AWS_KEEP_ALIVE_TIME";
        AWS_S3_FORCE_PATH_STYLE = "IIO_" + this.alias + "_AWS_FORCE_PATH_STYLE";

        URI uri = cogUri.getUri();

        // Step 1: Parse URL query parameters first (highest priority for inline config)
        Map<String, String> queryParams = parseQueryParams(uri);

        // Step 2: Parse the URI to get bucket and key
        S3URIParser parser = createParser(uri);
        if (parser == null) {
            throw new RuntimeException("Unable to parse the specified URI: " + uri);
        }
        this.bucket = parser.bucket;
        this.key = parser.key;
        this.filename = nameFromKey(key);

        // Step 3: Load configuration in priority order
        loadConfiguration(cogUri, queryParams, parser.region, parser.inferredEndpoint);

        // Validate region
        if (region == null || region.isEmpty()) {
            throw new RuntimeException("No region info found for bucket '" + bucket + "'. "
                    + "Please set via URL param (?region=...), registry, or environment variable '"
                    + AWS_S3_REGION_KEY + "'");
        }

        LOGGER.fine(() -> "S3 config for bucket '" + bucket + "': endpoint=" + endpoint
                + ", region=" + region + ", forcePathStyle=" + forcePathStyle);
    }

    /**
     * Loads configuration from multiple sources in priority order.
     */
    private void loadConfiguration(BasicAuthURI cogUri, Map<String, String> queryParams, String parsedRegion, String inferredEndpoint) {
        // Priority 1: URL query parameters
        String urlEndpoint = queryParams.get("endpoint");
        String urlRegion = queryParams.get("region");
        String urlPathStyle = queryParams.get("pathstyle");

        // Priority 2: Check bucket-specific registry
        Optional<S3StorageConfig> registryConfig = S3ConfigRegistry.getOrDefault(bucket);

        // Priority 3: Environment variables
        String envUser = PropertyLocator.getEnvironmentValue(AWS_S3_USER_KEY, null);
        String envPassword = PropertyLocator.getEnvironmentValue(AWS_S3_PASSWORD_KEY, null);
        String envEndpoint = PropertyLocator.getEnvironmentValue(AWS_S3_ENDPOINT_KEY, null);
        String envRegion = PropertyLocator.getEnvironmentValue(AWS_S3_REGION_KEY, null);
        String envPathStyle = PropertyLocator.getEnvironmentValue(AWS_S3_FORCE_PATH_STYLE, "false");
        int envCorePoolSize = Integer.parseInt(PropertyLocator.getEnvironmentValue(AWS_S3_CORE_POOL_SIZE_KEY, "50"));
        int envMaxPoolSize = Integer.parseInt(PropertyLocator.getEnvironmentValue(AWS_S3_MAX_POOL_SIZE_KEY, "128"));
        int envKeepAliveTime = Integer.parseInt(PropertyLocator.getEnvironmentValue(AWS_S3_KEEP_ALIVE_TIME, "10"));

        // Resolve endpoint (URL > Registry > Env > Inferred)
        if (urlEndpoint != null && !urlEndpoint.isEmpty()) {
            this.endpoint = urlEndpoint;
        } else if (registryConfig.isPresent() && registryConfig.get().getEndpoint() != null) {
            this.endpoint = registryConfig.get().getEndpoint();
        } else if (envEndpoint != null && !envEndpoint.isEmpty()) {
            this.endpoint = envEndpoint;
        } else {
            this.endpoint = inferredEndpoint;
        }

        // Resolve region (URL > Registry > Env > Parsed from URL)
        if (urlRegion != null && !urlRegion.isEmpty()) {
            this.region = urlRegion;
        } else if (registryConfig.isPresent() && registryConfig.get().getRegion() != null) {
            this.region = registryConfig.get().getRegion();
        } else if (envRegion != null && !envRegion.isEmpty()) {
            this.region = envRegion;
        } else {
            this.region = parsedRegion;
        }

        // Fallback: If region is still null and we have a custom endpoint (likely non-AWS),
        // default to "us-east-1" as the SDK requires a region.
        if (this.region == null && this.endpoint != null) {
            this.region = "us-east-1";
        }

        // Resolve credentials (BasicAuthURI > Registry > Env)
        if (cogUri.getUser() != null && cogUri.getPassword() != null) {
            this.user = cogUri.getUser();
            this.password = cogUri.getPassword();
        } else if (registryConfig.isPresent()
                && registryConfig.get().getAccessKeyId() != null
                && registryConfig.get().getSecretAccessKey() != null) {
            this.user = registryConfig.get().getAccessKeyId();
            this.password = registryConfig.get().getSecretAccessKey();
        } else {
            this.user = envUser;
            this.password = envPassword;
        }

        // Resolve path style (URL > Registry > Env)
        if (urlPathStyle != null) {
            this.forcePathStyle = Boolean.parseBoolean(urlPathStyle);
        } else if (registryConfig.isPresent()) {
            this.forcePathStyle = registryConfig.get().isForcePathStyle();
        } else {
            // If we are using a Generic/Inferred endpoint that isn't AWS, default to path style often works better,
            // but we adhere to the env var default.
            this.forcePathStyle = Boolean.parseBoolean(envPathStyle);
        }

        // Resolve pool settings (Registry > Env)
        if (registryConfig.isPresent()) {
            this.corePoolSize = registryConfig.get().getCorePoolSize();
            this.maxPoolSize = registryConfig.get().getMaxPoolSize();
            this.keepAliveTime = registryConfig.get().getKeepAliveTime();
        } else {
            this.corePoolSize = envCorePoolSize;
            this.maxPoolSize = envMaxPoolSize;
            this.keepAliveTime = envKeepAliveTime;
        }
    }

    /**
     * Parses query parameters from a URI.
     */
    private Map<String, String> parseQueryParams(URI uri) {
        Map<String, String> params = new HashMap<>();
        String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return params;
        }
        try {
            for (String pair : query.split("&")) {
                int idx = pair.indexOf("=");
                String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) : pair;
                String value = idx > 0 && pair.length() > idx + 1
                        ? URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
                        : "";
                params.put(key.toLowerCase(), value);
            }
        } catch (Exception e) {
            LOGGER.warning("Failed to parse query params from URI: " + uri + " - " + e.getMessage());
        }
        return params;
    }

    /**
     * Creates the appropriate URI parser based on scheme and host.
     */
    private S3URIParser createParser(URI uri) {
        String scheme = uri.getScheme().toLowerCase();

        if (scheme.startsWith("http")) {
            String host = uri.getHost();
            String hostLowerCase = host.toLowerCase();
            if ((hostLowerCase.startsWith(S3_DASH) || hostLowerCase.startsWith(S3_DOT)) && host.contains(".")) {
                return new HTTPPathStyleParser(uri, region, hostLowerCase.startsWith(S3_DASH));
            } else if (hostLowerCase.contains(S3_DASH_VH) || hostLowerCase.contains(S3_DOT_VH)) {
                return new HTTPVirtualHostedStyleParser(uri, region, hostLowerCase.contains(S3_DASH_VH));
            } else {
                // Fallback for generic S3-compatible URLs (e.g. MinIO, Wasabi with custom domains)
                return new GenericPathStyleParser(uri, region);
            }
        } else if (scheme.startsWith("s3")) {
            return new S3Parser(uri, region);
        }
        return null;
    }

    private String nameFromKey(String key) {
        String[] parts = key.split("/");
        return parts[parts.length - 1];
    }

    // Getters
    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getAlias() {
        return alias;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getFilename() {
        return filename;
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

    public boolean getForcePathStyle() {
        return forcePathStyle;
    }

    // --- Inner Parser Classes ---

    abstract static class S3URIParser {
        protected String region;
        protected String bucket;
        protected String key;
        protected String scheme;
        protected String host;
        protected String inferredEndpoint;
        protected URI uri;

        S3URIParser(URI uri, String defaultRegion) {
            this.uri = uri;
            this.region = defaultRegion;
            scheme = uri.getScheme().toLowerCase();
            host = uri.getHost();
        }
    }

    class HTTPPathStyleParser extends S3URIParser {
        HTTPPathStyleParser(URI uri, String defaultRegion, boolean isOldDashRegion) {
            super(uri, defaultRegion);
            // Infer endpoint from host
            this.inferredEndpoint = scheme + "://" + host;
            
            // Try to extract region, but don't fail if it's just a domain
            // Format: s3[-.]<region>.domain.com or s3.domain.com (default region)
            String hostRegion = null;
            if (host.startsWith(S3_DASH) || host.startsWith(S3_DOT)) {
                // remove prefix
                String withoutPrefix = host.substring(3);
                int firstDot = withoutPrefix.indexOf('.');
                if (firstDot > 0) {
                     hostRegion = withoutPrefix.substring(0, firstDot);
                }
            }
            
            if (hostRegion != null && !hostRegion.isEmpty()) {
                region = hostRegion;
            }
            
            String path = uri.getPath();
            path = path.startsWith("/") ? path.substring(1) : path;
            String[] parts = path.split("/", 2);
            if (parts.length >= 2) {
                bucket = parts[0];
                key = parts[1];
            } else {
                // Fallback/Error case
                bucket = parts[0];
                key = "";
            }
        }
    }

    class HTTPVirtualHostedStyleParser extends S3URIParser {
        HTTPVirtualHostedStyleParser(URI uri, String defaultRegion, boolean isOldDashRegion) {
            super(uri, defaultRegion);
            
            String s3Prefix = isOldDashRegion ? S3_DASH_VH : S3_DOT_VH;
            int s3Index = host.indexOf(s3Prefix);
            
            if (s3Index > 0) {
                bucket = host.substring(0, s3Index);
                String remainder = host.substring(s3Index + s3Prefix.length());
                
                // Check for region
                // AWS: bucket.s3.region.amazonaws.com -> remainder: region.amazonaws.com
                // Custom: bucket.s3.region.domain.com -> remainder: region.domain.com
                // Custom No Region: bucket.s3.domain.com -> remainder: domain.com
                
                int firstDot = remainder.indexOf('.');
                if (firstDot > 0) {
                    String potentialRegion = remainder.substring(0, firstDot);
                    // Simple heuristic: if the remainder has at least 2 dots, the first part is likely a region.
                    // e.g. eu-central-1.wasabisys.com
                    // If it has 1 dot: wasabisys.com -> 'wasabisys' is likely domain, but could be region?
                    // For now, we'll assume it's a region if it's not the *only* part of the domain.
                    // But 'amazonaws.com' is 2 parts.
                    
                    if (remainder.contains(".")) {
                         // Try to support standard AWS style logic which usually has a region here
                         // except for 's3.amazonaws.com' (legacy)
                         if (!remainder.equals("amazonaws.com")) {
                             region = potentialRegion;
                         }
                    }
                }
                
                // For virtual hosting, the endpoint is usually the suffix
                // e.g. https://s3.region.domain.com or https://domain.com
                // But S3Client expects the "service" endpoint.
                // If we extracted a region, we can try to reconstruct.
                // But safely, we can just use the scheme://host as endpoint? 
                // No, for virtual hosting, the endpoint passed to SDK should usually be the base.
                // If we pass https://bucket.s3.region.domain.com as endpoint override, SDK might get confused or double-bucket.
                // However, if we don't set endpoint override, it defaults to AWS.
                
                // Strategy: If we detect a custom provider (not amazonaws), we MUST set an endpoint.
                if (!host.endsWith(AMAZON_AWS)) {
                    // Construct endpoint from remainder?
                    // if host is bucket.s3.region.domain.com
                    // endpoint should ideally be https://s3.region.domain.com
                    this.inferredEndpoint = scheme + "://" + host.substring(s3Index + 1); // skip the dot before s3
                }
            }
            
            String path = uri.getPath();
            path = path.startsWith("/") ? path.substring(1) : path;
            key = path;
        }
    }
    
    class GenericPathStyleParser extends S3URIParser {
        GenericPathStyleParser(URI uri, String defaultRegion) {
            super(uri, defaultRegion);
            this.inferredEndpoint = scheme + "://" + host;
             if (uri.getPort() != -1) {
                this.inferredEndpoint += ":" + uri.getPort();
            }
            
            String path = uri.getPath();
            path = path.startsWith("/") ? path.substring(1) : path;
            String[] parts = path.split("/", 2);
            if (parts.length >= 2) {
                bucket = parts[0];
                key = parts[1];
            } else {
                bucket = parts[0];
                key = "";
            }
        }
    }

    class S3Parser extends S3URIParser {
        S3Parser(URI uri, String defaultRegion) {
            super(uri, defaultRegion);
            String path = uri.getPath();
            path = path.startsWith("/") ? path.substring(1) : path;
            bucket = uri.getHost();
            key = path;

            // Check for region in query params
            String query = uri.getQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    if (param.toLowerCase().startsWith("region=")) {
                        region = param.substring(7);
                        break;
                    }
                }
            }
        }
    }
}
