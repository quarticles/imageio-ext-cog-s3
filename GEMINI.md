# ImageIO-Ext COG S3 RangeReader (Quarticle Fork)

## Project Overview

This project is a specialized fork of the `imageio-ext-cog-rangereader-s3` library, designed to enable reading Cloud Optimized GeoTIFFs (COGs) from **S3-compatible storage providers** (e.g., Wasabi, MinIO, DigitalOcean Spaces). The original library hardcoded AWS endpoints, making it unusable with non-AWS S3 providers.

**Key Enhancements:**
*   **Configurable Endpoints:** Supports custom S3 endpoints via environment variables, URL parameters, or a programmatic registry.
*   **Flexible Configuration:** multiple layers of configuration with a defined priority order.
*   **AWS SDK v2:** Utilizes the modern asynchronous AWS SDK for Java.

## Architecture & Key Components

The core logic resides in `it.geosolutions.imageioimpl.plugins.cog`:

*   **`S3RangeReader.java`**: The main implementation of `AbstractRangeReader`. It handles the actual byte-range fetching logic using `S3AsyncClient`. It orchestrates the fetching of headers and specific byte ranges required for COG parsing.
*   **`S3ConfigurationProperties.java`**: A robust configuration handler. It resolves connection settings (endpoint, region, credentials, path-style access) from various sources.
*   **`S3ConfigRegistry.java`**: A static registry allowing developers to register specific configurations for different buckets programmatically.
*   **`S3ClientFactory.java`**: Responsible for instantiating the `S3AsyncClient` based on the resolved `S3ConfigurationProperties`.

## Configuration Hierarchy

The library resolves configuration in the following priority order (highest to lowest):

1.  **URL Query Parameters**: `s3://bucket/key?endpoint=...&region=...`
2.  **BasicAuthURI**: Credentials embedded in the URL (`s3://user:pass@bucket/key`).
3.  **Bucket-specific Registry**: Configurations registered via `S3ConfigRegistry`.
4.  **Environment Variables**: Global settings (e.g., `IIO_S3_AWS_ENDPOINT`).

## Building and Running

### Prerequisites
*   Java 11
*   Maven 3.x

### Build Commands

This project uses Maven for dependency management and building.

*   **Build and Install:**
    ```bash
    mvn clean install
    ```
*   **Run Tests:**
    ```bash
    mvn test
    ```

## Development Conventions

*   **Code Style:** The project follows standard Java conventions.
*   **Logging:** Uses `java.util.logging.Logger` (JUL) wrapped via SLF4J in dependencies.
*   **Asynchronous I/O:** The implementation relies on `CompletableFuture` and the AWS SDK's async client for non-blocking range reads.
*   **Package Structure:** `it.geosolutions.imageioimpl.plugins.cog` is the root package, maintaining compatibility with the original `imageio-ext` structure.

## Key Files

*   `README.md`: detailed usage instructions, including environment variables and XML configuration examples.
*   `pom.xml`: Maven build configuration, dependencies (AWS SDK, ImageIO Commons), and distribution settings.
*   `src/main/java/.../S3RangeReader.java`: Entry point for the reading logic.
