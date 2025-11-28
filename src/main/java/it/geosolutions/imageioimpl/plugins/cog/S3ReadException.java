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
 * Exception thrown when an error occurs while reading from S3.
 *
 * <p>This exception wraps underlying S3 SDK exceptions and provides
 * context about the operation that failed.
 */
public class S3ReadException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new S3ReadException with the specified message.
     *
     * @param message the error message
     */
    public S3ReadException(String message) {
        super(message);
    }

    /**
     * Creates a new S3ReadException with the specified message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public S3ReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
