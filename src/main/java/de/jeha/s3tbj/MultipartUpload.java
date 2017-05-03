package de.jeha.s3tbj;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author jenshadlich@googlemail.com
 */
public class MultipartUpload {

    private static final Logger LOG = LoggerFactory.getLogger(MultipartUpload.class);

    public static void main(String... args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        final String accessKey = "GMGR882QK9J3346TICDX";
        final String secretKey = "edd7NaBJWhPsVKue3eH89K337aQ6UNdBF83PZDNu";
        final String endpoint = "localhost:8888";
        final String bucket = "mp-upload";
        final String key = "test-object";

        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(Protocol.HTTP)
                .withUserAgentPrefix("s3tb")
                .withSignerOverride("S3SignerType"); // V2 signatures

        final AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfiguration);
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build());
        s3Client.setEndpoint(endpoint);

        if(!s3Client.doesBucketExist(bucket)) {
            s3Client.createBucket(bucket);
        }

        final List<PartETag> partETags = new ArrayList<>();

        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
        final InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

        final File file = new File("TODO");
        final long contentLength = file.length();
        long partSize = 5 * 1024 * 1024;

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be smaller. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));
                LOG.info("Uploading part {}, size={}", i, partSize);

                // Create request to upload a part.
                final UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucket).withKey(key)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // Upload part and add response to our list.
                partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            final CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(bucket,
                    key,
                    initResponse.getUploadId(),
                    partETags);

            s3Client.completeMultipartUpload(compRequest);
        } catch (Exception e) {
            LOG.error("Multipart upload failed", e);
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, initResponse.getUploadId()));
        }

    }

}
