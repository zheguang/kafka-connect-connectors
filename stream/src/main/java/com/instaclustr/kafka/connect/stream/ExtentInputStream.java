package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.endpoint.ExtentBased;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ExtentInputStream extends FilterInputStream {
    public static final long DEFAULT_MAX_EXTENT_SIZE = 1 << 26;  // 64MB

    private final String fileName;
    private final long fileSize;
    private final ExtentBased endpoint;

    public final long extentStride; // Last extent may be less
    private long extentStartOffset;
    private long fileOffset;

    ExtentInputStream(String fileName,
                      long fileSize,
                      ExtentBased endpoint,
                      long extentStride,
                      long extentStartOffset,
                      long fileOffset) {
        super(null);
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.endpoint = endpoint;
        this.extentStride = extentStride;
        this.extentStartOffset = extentStartOffset;
        this.fileOffset = fileOffset;
    }

    public static ExtentInputStream of(String fileName, long fileSize, ExtentBased endpoint) {
        return of(fileName, fileSize, endpoint, DEFAULT_MAX_EXTENT_SIZE);
    }

    public static ExtentInputStream of(String fileName, long fileSize, ExtentBased endpoint, long maxExtentSize) {
        return new ExtentInputStream(fileName, fileSize, endpoint, maxExtentSize, 0, 0);
    }

    @Override
    public int read() throws IOException {
        if (! isExtentOpen()) {
            if (! hasExtentAt(extentStartOffset)) {
                return -1;
            }
            openExtent();
        }
        int result;
        while ((result = in.read()) == -1 && hasNextExtent()) {
            setNextExtent();
        }
        fileOffset++;
        return result;
    }

    @Override
    public int available() throws IOException {
        if (! isExtentOpen()) {
            if (! hasExtentAt(extentStartOffset)) {
                return 0;
            }
            openExtent();
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
        extentStartOffset = offset;
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
        return Math.min(extentStride, fileSize - (offset + extentStride));
    }

    public long getStreamOffset() {
        return fileOffset;
    }
}
