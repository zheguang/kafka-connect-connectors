package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.endpoint.ExtentBased;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ExtentInputStream extends FilterInputStream {
    public static final long DEFAULT_EXTENT_STRIDE = 1024 * 1024;
    public static Logger log = LoggerFactory.getLogger(ExtentInputStream.class);

    private final String fileName;
    private final long fileSize;
    private final ExtentBased endpoint;

    public final long extentStride; // Expected extent size; last one may be less
    private long extentSize; // Actual size of extent; equal stride except possibly the last one
    private long extentStartOffset;
    private long extentPosition; // byte position within extent
    private long fileOffset;

    ExtentInputStream(InputStream in,
                      String fileName,
                      long fileSize,
                      ExtentBased endpoint,
                      long extentStride,
                      long extentSize,
                      long extentStartOffset,
                      long extentPosition,
                      long fileOffset) {
        super(in);
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.endpoint = endpoint;
        this.extentStride = extentStride;
        this.extentSize = extentSize;
        this.extentStartOffset = extentStartOffset;
        this.extentPosition = extentPosition;
        this.fileOffset = fileOffset;
    }

    public static ExtentInputStream of(String fileName, long fileSize, ExtentBased endpoint) {
        return of(fileName, fileSize, endpoint, DEFAULT_EXTENT_STRIDE);
    }

    public static ExtentInputStream of(String fileName, long fileSize, ExtentBased endpoint, long maxExtentSize) {
        return new ExtentInputStream(null, fileName, fileSize, endpoint, maxExtentSize,
                0, 0, 0, 0);
    }

    /*@Override
    public int read() throws IOException {
        if (! isExtentOpen()) {
            if (! hasExtentAt(extentStartOffset)) {
                return -1;
            }
            openExtent();
        }
        int result;
        while ((result = extentRead()) == -1 && hasNextExtent()) {
            log.debug("Setting next extent");
            setNextExtent();
        }
        if (result != -1) {
            extentPosition++;
            fileOffset++;
        }
        return result;
    }*/

    @Override
    public int read() throws IOException {
        throw new IOException("Unsupported");
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (! isExtentOpen()) {
            if (! hasExtentAt(extentStartOffset)) {
                return -1;
            }
            openExtent();
        }
        int result;
        while ((result = extentRead(b, off, len)) == -1 && hasNextExtent()) {
            log.debug("Setting next extent");
            setNextExtent();
        }
        if (result != -1) {
            extentPosition += result;
            fileOffset += result;
        }
        return result;
    }

    // -1 if no end of extent
    private int extentRead(byte[] b, int off, int len) throws IOException {
        if (expectEndOfExtent()) {
            return -1;
        } else {
            assert extentPosition < extentSize;
            return in.read(b, off, len);
        }
    }

    // -1 if no end of extent
    /*private int extentRead() throws IOException {
        if (expectEndOfExtent()) {
            return -1;
        } else {
            assert extentPosition < extentSize;
            int result = in.read();
            assert result != -1;
            return result;
        }
    }*/

    private boolean expectEndOfExtent() {
        return extentPosition == extentSize;
    }

    @Override
    public int available() throws IOException {
        if (! isExtentOpen()) {
            if (! hasExtentAt(extentStartOffset)) {
                return 0;
            }
            // Estimate size to be extent size
            return (int) getExtentSizeAt(extentStartOffset);
        }
        return extentAvailable();
    }

    private int extentAvailable() throws IOException {
        if (expectEndOfExtent()) {
            if (hasNextExtent()) {
                // Estimate size to be extent size
                return (int) getExtentSizeAt(extentStartOffset + extentSize);
            }
            return 0; // End of whole stream
        }
        return in.available();
    }

    @Override
    public long skip(long length) throws IOException {
        // Only efficient when skipping across extents. Inefficient when skipping within an extent
        long newOffset = Math.min(extentStartOffset + length, fileSize);
        if (isExtentOpen()) {
            closeExtent();
        }
        long skipped = newOffset - extentStartOffset;
        if (hasExtentAt(newOffset)) {
            setExtentAt(newOffset);
        } else {
            extentStartOffset = newOffset;
        }
        return skipped;
    }

    @Override
    public void close() throws IOException {
        if (isExtentOpen()) {
            closeExtent();
        }
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public long getExtentStartOffset() {
        return extentStartOffset;
    }

    private boolean isExtentOpen() {
        return in != null;
    }

    private void openExtent() throws IOException {
        setExtentAt(extentStartOffset);
    }

    private void setNextExtent() throws IOException {
        setExtentAt(extentStartOffset + extentStride);
    }

    private void setExtentAt(long offset) throws IOException {
        InputStream newExtent = getExtentAt(offset);
        closeExtent();
        in = newExtent;
        extentSize = getExtentSizeAt(offset);
        extentStartOffset = offset;
        extentPosition = 0;
        fileOffset = offset;
    }

    private void closeExtent() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            in = null;
        }
    }

    private boolean hasNextExtent() {
        return hasExtentAt(extentStartOffset + extentStride);
    }

    private boolean hasExtentAt(long offset) {
        return offset < fileSize;
    }

    private InputStream getExtentAt(long offset) throws IOException {
        assert hasExtentAt(offset);
        return endpoint.openInputStream(fileName, offset, getExtentSizeAt(offset));
    }

    private long getExtentSizeAt(long offset) {
        return Math.min(extentStride, fileSize - offset);
    }

    public long getStreamOffset() {
        return fileOffset;
    }
}
