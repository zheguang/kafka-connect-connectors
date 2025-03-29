package com.instaclustr.kafka.connect.stream.codec;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class CharDecoderTest {

    private CharDecoder decoder;
    private File tempFile;

    @BeforeMethod
    public void setup() throws IOException {
        tempFile = File.createTempFile("CharDecoderTest-tempFile", null);
    }

    @AfterMethod
    public void teardown() throws IOException {
        Files.deleteIfExists(tempFile.toPath());
        if (decoder != null) {
            decoder.close();
        }
    }

    @Test
    public void testEmptyLine() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String line = "\nfoo\n";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(line.getBytes(charset));
            os.flush();
        }

        List<CharRecord> records = decoder.next(2);
        assertEquals(records.size(), 2);

        CharRecord record = records.get(0);
        assertEquals(record.getRecord(), "");
        assertEquals(record.getStreamOffset(), 1);

        record = records.get(1);
        assertEquals(record.getRecord(), "foo");
        assertEquals(record.getStreamOffset(), 5);
    }

    @Test
    public void testNonTerminatedLine() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String line = "foo";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(line.getBytes(charset));
            os.flush();
        }

        List<CharRecord> records = decoder.next(2);
        assertNull(records);
    }

    @Test
    public void testSeek() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String line = "first line\n";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(line.getBytes(charset));
            os.flush();
        }

        int expectedOneCharInBytes = 1;
        int skipOneChar = 1;
        int skipOneCharInBytes = line.substring(0, skipOneChar).getBytes(charset).length;
        assertEquals(skipOneCharInBytes, expectedOneCharInBytes);
        decoder.skipFirstBytes(skipOneCharInBytes);

        List<CharRecord> records = decoder.next(1);
        assertEquals(records.size(), 1);

        CharRecord record = records.get(0);
        String expectedRecord = "irst line";
        assertEquals(record.getRecord(), expectedRecord);
        assertEquals(record.getStreamOffset(), line.getBytes(charset).length);
    }

    @Test
    public void testBatchSize() throws IOException {
        int batchSize = 5000;
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset, 2);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            writeTimesAndFlush(os, 10_000,
                    "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...\n".getBytes()
            );

            assertEquals(decoder.bufferSize(), 2);
            List<CharRecord> records = decoder.next(batchSize);
            assertEquals(records.size(), 5000);
            assertEquals(decoder.bufferSize(), 128);

            records = decoder.next(batchSize);
            assertEquals(records.size(), 5000);
            assertEquals(decoder.bufferSize(), 128);
        }
    }

    @Test
    public void testBufferResize() throws IOException {
        int batchSize = 1000;
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset, 2);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            assertEquals(decoder.bufferSize(), 2);

            writeAndAssertBufferSize(batchSize, os, "1\n".getBytes(charset), 2);
            writeAndAssertBufferSize(batchSize, os, "333\n".getBytes(charset), 4);
            writeAndAssertBufferSize(batchSize, os, "66666\n".getBytes(charset), 8);
            writeAndAssertBufferSize(batchSize, os, "7777777\n".getBytes(charset), 8);
            writeAndAssertBufferSize(batchSize, os, "88888888\n".getBytes(charset), 16);
            writeAndAssertBufferSize(batchSize, os, "999999999\n".getBytes(charset), 16);

            byte[] bytes = new byte[1025];
            Arrays.fill(bytes, (byte) '*');
            bytes[1024] = '\n';
            writeAndAssertBufferSize(batchSize, os, bytes,2048);
            writeAndAssertBufferSize(batchSize, os, "4444\n".getBytes(charset),2048);
        }
    }

    @Test
    public void testUtf16() throws IOException {
        Map<String, String> props = new HashMap<>();
        props.put(CharDecoder.CHARACTER_SET, "UTF-16");
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), props);

        Charset expectedCharset = StandardCharsets.UTF_16;
        assertEquals(decoder.getCharset(), expectedCharset);

        String line = "foo\nbar\n";
        byte[] lineBytes = line.getBytes(expectedCharset);
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(lineBytes);
            os.flush();
        }

        List<CharRecord> records = decoder.next(2);
        assertEquals(records.size(), 2);
        assertEquals(records.get(0).getRecord(), "foo");
        assertEquals(records.get(0).getStreamOffset(), 2 + 4 * 2);
        assertEquals(records.get(1).getRecord(), "bar");
        assertEquals(records.get(1).getStreamOffset(), 2 + 4 * 2 + 4 * 2);
    }

    private void writeAndAssertBufferSize(int batchSize, OutputStream os, byte[] bytes, int expectBufferSize)
            throws IOException {
        writeTimesAndFlush(os, batchSize, bytes);
        List<CharRecord> records = decoder.next(batchSize);
        assertEquals(batchSize, records.size());
        String expectedLine = new String(bytes, 0, bytes.length - 1); // remove \n
        for (CharRecord record : records) {
            assertEquals(record.getRecord(), expectedLine);
        }
        assertEquals(decoder.bufferSize(), expectBufferSize);
    }

    private void writeTimesAndFlush(OutputStream os, int times, byte[] line) throws IOException {
        for (int i = 0; i < times; i++) {
            os.write(line);
        }
        os.flush();
    }
}
