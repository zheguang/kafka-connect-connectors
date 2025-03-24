package com.instaclustr.kafka.connect.stream.endpoint;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.instaclustr.kafka.connect.stream.Endpoint;
import org.apache.kafka.common.config.AbstractConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class S3BucketOntap extends S3Bucket implements AccessKeyBased {

    private S3BucketOntap(TransferManager transferManager, String bucketName) {
        super(transferManager, bucketName);
    }

    public static S3BucketOntap of(Map<String, String> providedConf) {
        AbstractConfig s3BucketConf = new AbstractConfig(S3Bucket.CONFIG_DEF, providedConf);
        AbstractConfig accessKeyConf = new AbstractConfig(AccessKeyBased.CONFIG_DEF, providedConf);

        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(accessKeyConf.getString(ACCESS_KEY_ID), accessKeyConf.getPassword(ACCESS_KEY).value())
        );

        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                s3BucketConf.getString(S3Bucket.URL),
                s3BucketConf.getString(S3Bucket.REGION)
        );

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(awsStaticCredentialsProvider)
                .withClientConfiguration(
                        new ClientConfiguration()
                                .withMaxErrorRetry(5)
                                .withTcpKeepAlive(true))
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpointConfiguration);

        TransferManagerBuilder transferBuilder = TransferManagerBuilder.standard()
                .withS3Client(
                        clientBuilder.build()
                );

        return new S3BucketOntap(transferBuilder.build(), s3BucketConf.getString(BUCKET_NAME));
    }
}
