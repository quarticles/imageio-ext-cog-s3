/*
 *    ImageI/O-Ext - OpenSource Java Image translation Library
 *    https://github.com/quarticles/imageio-ext-cog-s3
 *    (C) 2024, Quarticle
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

/**
 * Exception thrown when an S3 object does not exist.
 *
 * <p>This exception is thrown when attempting to read a file from S3 that
 * does not exist (HTTP 404 / NoSuchKeyException). This allows callers to
 * distinguish between "file not found" and other read errors.
 *
 * <p>Common use cases include checking for optional files like external
 * overview files (.ovr), where the absence of the file should not be
 * treated as a fatal error.
 */
public class S3FileNotFoundException extends S3ReadException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new S3FileNotFoundException with the specified message.
     *
     * @param message the error message
     */
    public S3FileNotFoundException(String message) {
        super(message);
    }

    /**
     * Creates a new S3FileNotFoundException with the specified message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause (typically NoSuchKeyException)
     */
    public S3FileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
