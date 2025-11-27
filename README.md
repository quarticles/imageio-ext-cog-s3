# ImageIO-Ext COG S3 RangeReader (Quarticle Fork)

Fork of the [imageio-ext](https://github.com/geosolutions-it/imageio-ext) COG S3 RangeReader with support for S3-compatible storage providers.

## Why This Fork?

The original `imageio-ext-cog-rangereader-s3` library hardcodes `.amazonaws.com` in the `getURL()` method, which breaks compatibility with S3-compatible storage providers like:

- **Wasabi** (`s3.wasabisys.com`)
- **MinIO**
- **DigitalOcean Spaces**
- **Backblaze B2**
- Other S3-compatible providers

This fork adds configurable endpoint support via environment variables, URL query parameters, or a programmatic registry.

## Features

- Configurable S3 endpoint for S3-compatible providers
- Per-bucket configuration via static registry
- URL query parameter support for inline configuration
- Path-style and virtual-hosted-style URL support
- Backward compatible with standard AWS S3

## Installation

### Maven

Add the GitHub Packages repository and dependency to your `pom.xml`:

```xml
<repositories>
  <repository>
    <id>github</id>
    <url>https://maven.pkg.github.com/quarticles/imageio-ext-cog-s3</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>io.github.quarticles</groupId>
    <artifactId>imageio-ext-cog-rangereader-s3</artifactId>
    <version>2.0.1-quarticle-1</version>
  </dependency>
</dependencies>
```

**Important**: Exclude the original `imageio-ext-cog-rangereader-s3` from your dependencies to avoid conflicts:

```xml
<dependency>
  <groupId>it.geosolutions.imageio-ext</groupId>
  <artifactId>imageio-ext-cog-commons</artifactId>
  <version>${imageio-ext.version}</version>
  <exclusions>
    <exclusion>
      <groupId>it.geosolutions.imageio-ext</groupId>
      <artifactId>imageio-ext-cog-rangereader-s3</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

## Configuration

### Method 1: Environment Variables

Set environment variables for global configuration:

```bash
# Endpoint for S3-compatible provider
export IIO_S3_AWS_ENDPOINT=https://s3.eu-central-1.wasabisys.com

# Region
export IIO_S3_AWS_REGION=eu-central-1

# Credentials (optional - uses AWS default chain if not set)
export IIO_S3_AWS_USER=your-access-key
export IIO_S3_AWS_PASSWORD=your-secret-key

# Use path-style URLs (required for most S3-compatible providers)
export IIO_S3_AWS_FORCE_PATH_STYLE=true
```

### Method 2: URL Query Parameters

Pass configuration inline via URL query parameters:

```
s3://my-bucket/path/to/file.tif?endpoint=https://s3.eu-central-1.wasabisys.com&region=eu-central-1&pathStyle=true
```

### Method 3: Programmatic Registry

Register bucket-specific configurations programmatically:

```java
import it.geosolutions.imageioimpl.plugins.cog.S3ConfigRegistry;
import it.geosolutions.imageioimpl.plugins.cog.S3StorageConfig;

// Register Wasabi configuration for a specific bucket
S3ConfigRegistry.register("my-wasabi-bucket",
    S3StorageConfig.builder()
        .endpoint("https://s3.eu-central-1.wasabisys.com")
        .region("eu-central-1")
        .credentials("access-key", "secret-key")
        .forcePathStyle(true)
        .build());

// Register MinIO configuration
S3ConfigRegistry.register("my-minio-bucket",
    S3StorageConfig.builder()
        .endpoint("http://minio.local:9000")
        .region("us-east-1")
        .credentials("minio-user", "minio-pass")
        .forcePathStyle(true)
        .build());

// Set a default configuration for unregistered buckets
S3ConfigRegistry.setDefault(
    S3StorageConfig.builder()
        .endpoint("https://s3.eu-central-1.wasabisys.com")
        .region("eu-central-1")
        .forcePathStyle(true)
        .build());
```

## Configuration Priority

When multiple configuration sources are present, they are resolved in this order:

1. **URL query parameters** (highest priority)
2. **BasicAuthURI credentials** (user:pass in URL)
3. **Bucket-specific registry entry**
4. **Default registry configuration**
5. **Environment variables** (lowest priority)

## Usage with GeoServer Cloud

To use this fork with GeoServer Cloud:

1. Exclude the original library from raster-formats dependencies
2. Add this fork as a dependency
3. Configure via environment variables or the registry

See the [GeoServer Cloud](https://github.com/quarticles/geoserver-cloud) fork for integration details.

## License

LGPL v3.0 - Same as the original imageio-ext library.

## Credits

- Original library by [GeoSolutions](https://github.com/geosolutions-it/imageio-ext)
- Fork maintained by [Quarticle](https://github.com/quarticles)
