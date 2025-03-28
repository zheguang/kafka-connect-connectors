package com.instaclustr.kafka.connect.stream.endpoint;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.instaclustr.kafka.connect.stream.Endpoint;
import com.instaclustr.kafka.connect.stream.ExtentInputStream;
import org.apache.kafka.common.config.ConfigDef;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public abstract class S3Bucket implements Endpoint, ExtentBased {
    public static String BUCKET_NAME = "s3.bucket.name";
    public static String REGION = "s3.region";
    public static String URL = "s3.url";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BUCKET_NAME, STRING, HIGH, "S3 bucket name")
            .define(REGION, STRING, HIGH, "S3 Region")
            .define(URL, STRING, "", HIGH, "S3 URL. Default to AWS's endpoint");

    private final TransferManager transferManager;
    private final String bucketName;

    public S3Bucket(TransferManager transferManager, String bucketName) {
        this.transferManager = transferManager;
        this.bucketName = bucketName;
    }

    @Override
    public InputStream openInputStream(String objectKey) throws IOException {
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(bucketName, objectKey);
            long objectSize = metadata.getContentLength();
            return ExtentInputStream.of(objectKey, objectSize, this);
        } catch (SdkClientException e) {
            throw new IOException (e);
        }
    }

    @Override
    public InputStream openInputStream(String objectKey, long start, long length) throws IOException {
        try {
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, objectKey)
                    .withRange(start, start + length - 1);
            return Objects.requireNonNull(
                    getClient().getObject(getObjectRequest).getObjectContent(),
                    "No stream found");
        } catch (SdkClientException e) {
            throw new IOException (e);
        }
    }

    private AmazonS3 getClient() {
        return transferManager.getAmazonS3Client();
    }
}
