package de.jeha.s3tbj;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author jenshadlich@googlemail.com
 */
public class CountObjects {

    private static final Logger LOG = LoggerFactory.getLogger(CountObjects.class);

    public static void main(String... args) throws Exception {
        Locale.setDefault(Locale.ENGLISH);
/*
        final Properties properties = UserProperties.fromHome().load("s3tbj");
        final String accessKey = properties.getProperty("accessKey");
        final String secretKey = properties.getProperty("secretKey");
        final String endpoint = properties.getProperty("endpoint");
*/
        final String accessKey = "GMGR882QK9J3346TICDX";
        final String secretKey = "edd7NaBJWhPsVKue3eH89K337aQ6UNdBF83PZDNu";
        final String endpoint = "localhost:8888";
        final String bucket = "bucket-with-versioning";

        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(Protocol.HTTP)
                .withUserAgentPrefix("s3tbj")
                .withSignerOverride("S3SignerType");

        final AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfiguration);
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build());
        s3Client.setEndpoint(endpoint);

        final ExecutorService pool = Executors.newFixedThreadPool(2);

        final Collection<Callable<Integer>> tasks = new LinkedList<>();

        tasks.add(() -> countObjects(s3Client, bucket, null));
        final List<Future<Integer>> results = pool.invokeAll(tasks);

        pool.shutdown();

        System.out.println();
        LOG.info("total #objects: {}", results.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).reduce((a, b) -> a + b).orElse(-1));
    }

    // -----------------------------------------------------------------------------------------------------------------

    private static int countObjects(AmazonS3 s3Client, String bucket, String prefix) {
        int objectCount = 0;
        int chunks = 0;

        LOG.info("Start: bucket '{}', prefix '{}'", bucket, prefix);

        boolean hasMoreResults;
        ObjectListing previousObjectListing = null;
        do {
            //printProgress(++chunks);

            ObjectListing objectListing = (previousObjectListing != null)
                    ? s3Client.listNextBatchOfObjects(previousObjectListing)
                    : s3Client.listObjects(bucket, prefix);
            previousObjectListing = objectListing;
            hasMoreResults = objectListing.isTruncated();

            objectCount += objectListing.getObjectSummaries().size();
        } while (hasMoreResults);

        LOG.info("#objects: {}", objectCount);
        return objectCount;
    }

    private static void printProgress(int i) {
        System.out.print(".");
        System.out.flush();
        if (i % 100 == 0) {
            System.out.println();
        }
    }

}
