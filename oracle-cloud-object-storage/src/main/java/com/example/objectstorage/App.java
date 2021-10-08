package com.example.objectstorage;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.CreateBucketRequest;
import com.oracle.bmc.objectstorage.requests.DeleteBucketRequest;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.CreateBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Hello world!
 */
public class App {

    public static final String FILE_URL = "https://objectcomputing.com/files/2716/2256/3799/micronaut_stacked_black.png";

    public static void main(String[] args) throws IOException {

        String compartmentId = System.getenv("COMPARTMENT_ID");

        // Configuring the AuthenticationDetailsProvider. It's assuming there is a default OCI config file
        // "~/.oci/config", and a profile in that config with the name "DEFAULT". Make changes to the following
        // line if needed and use ConfigFileReader.parse(configurationFilePath, profile);

        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault();

        final AuthenticationDetailsProvider provider =
                new ConfigFileAuthenticationDetailsProvider(configFile);

        ObjectStorage client = new ObjectStorageClient(provider);
        client.setRegion(Region.US_PHOENIX_1);

        GetNamespaceResponse namespaceResponse =
                client.getNamespace(GetNamespaceRequest.builder().build());
        String namespaceName = namespaceResponse.getValue();
        System.out.println("Using namespace: " + namespaceName);

        String bucket = "bucket" + System.currentTimeMillis();
        String key = "micronaut-logo";

        tutorialSetup(client, compartmentId, namespaceName, bucket);

        System.out.println("Uploading object...");

        URL url = new URL(FILE_URL);
        byte[] output = downloadUrl(url);

        uploadObject(client, namespaceName, bucket, key, output);

        System.out.println("Upload complete");
        System.out.printf("%n");

        cleanUp(client, namespaceName, bucket, key);
        System.out.println("Exiting...");
    }

    public static void uploadObject(ObjectStorage client, String namespaceName, String bucketName, String objectName, byte[] content) throws IOException {

        client.putObject(PutObjectRequest.builder()
                .objectName(objectName)
                .bucketName(bucketName)
                .namespaceName(namespaceName)
                .putObjectBody(new ByteArrayInputStream(content))
                .build());
        System.out.println("Content uploaded to bucket " + bucketName + " as " + objectName);
    }

    public static void tutorialSetup(ObjectStorage client, String compartmentId, String namespaceName, String bucketName) {

        System.out.println("Creating bucket: " + bucketName);

        // Instantiate container client
        CreateBucketResponse createBucketResponse = client.createBucket(
                CreateBucketRequest.builder()
                        .namespaceName(namespaceName)
                        .createBucketDetails(
                                CreateBucketDetails.builder()
                                        .compartmentId(compartmentId)
                                        .name(bucketName)
                                        .build()
                        ).build()
        );

        System.out.println(bucketName + " is ready. " + createBucketResponse.getLocation());
        System.out.printf("%n");

    }

    public static void cleanUp(ObjectStorage client, String namespaceName, String bucketName, String keyName) {
        System.out.println("Cleaning up...");

        System.out.println("Deleting object: " + keyName);
        client.deleteObject(DeleteObjectRequest.builder()
                .bucketName(bucketName)
                .namespaceName(namespaceName)
                .objectName(keyName)
                .build());
        System.out.println(keyName + " has been deleted. ");
        System.out.println("Deleting bucket: " + bucketName);

        client.deleteBucket(DeleteBucketRequest.builder()
                .bucketName(bucketName)
                .namespaceName(namespaceName)
                .build());
        System.out.println(bucketName + " has been deleted.");
        System.out.printf("%n");

        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }

    private static byte[] downloadUrl(URL toDownload) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int bytesRead;
        InputStream stream = toDownload.openStream();

        while ((bytesRead = stream.read(chunk)) > 0) {
            outputStream.write(chunk, 0, bytesRead);
        }
        return outputStream.toByteArray();
    }
}
