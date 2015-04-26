/*-----------------------------------------------------------------------------
 * Copyright © 2015 Keith Webster Johnston.
 * All rights reserved.
 *
 * This file is part of blobs-s3.
 *
 * wiki-app is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * blobs-s3 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with blobs-s3. If not, see <http://www.gnu.org/licenses/>.
 *---------------------------------------------------------------------------*/
package com.johnstok.blobs.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.UUID;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.johnstok.blobs.ByteStore;
import com.johnstok.blobs.ByteStoreException;


/**
 * A {@link ByteStore} implementation using S3.
 *
 * @author Keith Webster Johnston.
 */
public class S3ByteStore
    implements
        ByteStore {

    private final AmazonS3 _s3Client;
    private final String _bucket;


    /**
     * Constructor.
     *
     * @param s3Client The S3 client this byte store will use.
     * @param bucket   The S3 bucket this byte store will use.
     */
    public S3ByteStore(final AmazonS3 s3Client, final String bucket) {
        _s3Client = Objects.requireNonNull(s3Client);
        _bucket = Objects.requireNonNull(bucket);
    }


    /** {@inheritDoc}  */
    @Override
    public UUID create(final InputStream in) throws ByteStoreException {
        final UUID id = UUID.randomUUID();
        update(id, in);
        return id;
    }


    /** {@inheritDoc}  */
    @Override
    public void update(final UUID id, final InputStream in) throws ByteStoreException {
        Objects.requireNonNull(in);
        Objects.requireNonNull(id);
        try {
            /*
FIXME: In general, when your object size reaches 100 MB, you should consider
using multipart uploads instead of uploading the object in a single operation.

http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html
             */
            _s3Client.putObject(
                new PutObjectRequest(
                    _bucket,
                    id.toString(),
                    in,
                    new ObjectMetadata()));
        } catch (final AmazonClientException e) {
            throw new ByteStoreException(e);
        }
    }


    /** {@inheritDoc} */
    @Override
    public void delete(final UUID id) throws ByteStoreException {
        Objects.requireNonNull(id);
        try {
            _s3Client.deleteObject(new DeleteObjectRequest(_bucket, id.toString()));
        } catch (final AmazonClientException e) {
            throw new ByteStoreException(e);
        }
    }


    /** {@inheritDoc} */
    @Override
    public void read(final UUID id, final OutputStream out) throws ByteStoreException {
        Objects.requireNonNull(id);
        Objects.requireNonNull(out);
        try (final S3Object object = _s3Client.getObject(new GetObjectRequest(_bucket, id.toString()))) {
            copy(object.getObjectContent(), out);
        } catch (AmazonClientException | IOException e) {
            throw new ByteStoreException(e);
        }
    }

    private static void copy(final InputStream  is,
                             final OutputStream os) throws IOException { // TODO: factor out.
        final byte[] buffer = new byte[1024];
        int read = is.read(buffer);
        while (0<read) {
            os.write(buffer, 0, read);
            read = is.read(buffer);
        }
    }
}
