package com.instaclustr.kafka.connect.stream.codec;

import com.instaclustr.kafka.connect.stream.codec.Record;
import com.instaclustr.kafka.connect.stream.endpoint.LocalFile;
import com.instaclustr.kafka.connect.stream.RandomAccessInputStream;
import com.instaclustr.kafka.connect.stream.types.parquet.ParquetKafkaDataConverter;
import com.instaclustr.kafka.connect.stream.types.parquet.StreamInputFile;
import org.apache.kafka.connect.data.Struct;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.StreamParquetReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.testng.Assert.*;

public class ParquetDecoderTest {
    
    private ParquetDecoder decoder;
    private File tempFile;

    private static final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(makeUsers(1000));

    private static List<PhoneBookWriter.User> makeUsers(int rowCount) {
        List<PhoneBookWriter.User> users = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            PhoneBookWriter.Location location = null;
            if (i % 3 == 1) {
                location = new PhoneBookWriter.Location((double) i, (double) i * 2);
            }
            if (i % 3 == 2) {
                location = new PhoneBookWriter.Location((double) i, null);
            }
            // row index of each row in the file is same as the user id.
            users.add(new PhoneBookWriter.User(
                        i, "p" + i, Arrays.asList(new PhoneBookWriter.PhoneNumber(i, "cell")), location));
        }
        return users;
    }

    private static void writePhoneBookToFile(Path file, ParquetProperties.WriterVersion parquetVersion)
            throws IOException {
            int pageSize = DATA.size() / 10; // Ensure that several pages will be created
            int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

            PhoneBookWriter.write(
                    ExampleParquetWriter.builder(file)
                    .withWriteMode(OVERWRITE)
                    .withRowGroupSize(rowGroupSize)
                    .withPageSize(pageSize)
                    .withWriterVersion(parquetVersion),
                    DATA);
    }

    @BeforeMethod
    public void setup() throws IOException {
        tempFile = File.createTempFile("ParquetDecoderTest-tempFile", ".parquet");
        writePhoneBookToFile(new Path(tempFile.getAbsolutePath()), ParquetProperties.WriterVersion.PARQUET_2_0);

        if (decoder != null) {
            decoder.close();
        }
        decoder = null;
    }

    @AfterMethod
    public void teardown() throws IOException {
        Files.deleteIfExists(tempFile.toPath());
        if (decoder != null) {
            decoder.close();
        }
    }

    @Test
    public void decode() throws IOException {
        // Use LocalFile to open an RandomAccessInputStream from tempFile
        LocalFile localFile = new LocalFile();
        RandomAccessInputStream rais = localFile.openRandomAccessInputStream(tempFile.getAbsolutePath());
        decoder = ParquetDecoder.from(rais);

        System.out.println("Start decoding");
        List<Record<Struct>> batch = decoder.next(1);
        assertEquals(batch.size(), 1);
        System.out.println(batch.get(0));
        System.out.println(DATA.get(0));
    }

    @Test
    public void decodeLocal() throws IOException {
        // File myFile = new File("/Users/guang/Play/KafkaConnect/kafka-connect-converters/kafka-parquet/src/test/resources/test.parquet");
        // RandomAccessInputStream rais = localFile.openRandomAccessInputStream(myFile.getAbsolutePath());
        File file = new File("/Users/guang/Play/KafkaConnect/kafka-connect-converters/kafka-parquet/src/test/resources/test.parquet");
        SeekableInputStream fileStream = new LocalInputFile(file.toPath()).newStream();
        RandomAccessInputStream rais = new RandomAccessInputStream(fileStream) {
            @Override
            public void seek(final long offset) throws IOException {
                fileStream.seek(offset);
            }

            @Override
            public long getStreamOffset() throws IOException {
                return fileStream.getPos();
            }

            @Override
            public long getSize() {
                return file.length();
            }
        };
        decoder = ParquetDecoder.from(rais);

        System.out.println("Start decoding");
        List<Record<Struct>> batch = decoder.next(1);
        assertEquals(batch.size(), 1);
        System.out.println(batch.get(0));
    }

    @Test
    public void decodeLocal2() throws IOException {
        File file = new File("/Users/guang/Play/KafkaConnect/kafka-connect-converters/kafka-parquet/src/test/resources/test.parquet");
        SeekableInputStream fileStream = new LocalInputFile(file.toPath()).newStream();

        StreamParquetReader reader = new StreamParquetReader(new StreamInputFile(() -> fileStream, file.length()));

        int i = 0;
        while (true) {
            System.out.println("Progress: " + reader.getProgress());
            SimpleGroup record = (SimpleGroup) reader.read();
            System.out.println("\nParquet record " + i + ": " + record);
            if (record == null) {
                break;
            }

            ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
            Struct struct = converter.convert(record);

            SourceRecord sourceRecord = new SourceRecord(
                    null,
                    null,
                    "mytopic",
                    null,
                    null,
                    null,
                    struct.schema(),
                    struct,
                    System.currentTimeMillis()
            );

            System.out.println("Kafka record " + i + ": " + sourceRecord);
            i++;
        }

        System.out.println("Close reader");
        reader.close();

    }
}
