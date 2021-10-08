package com.example.objectstorage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/storage/azure-storage-blob/src/samples/java/com/azure/storage/blob/BasicExample.java
 */
public class App {
    public static final String FILE_URL = "https://objectcomputing.com/files/2716/2256/3799/micronaut_stacked_black.png";

    public static void main(String[] args) throws IOException {
        String storageAccountConnectionString = System.getenv("STORAGE_ACCOUNT_CONNECTION_STRING");

        // Instantiated blob service client
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(storageAccountConnectionString)
                .buildClient();

        String bucket = "bucket" + System.currentTimeMillis();
        String key = "micronaut-logo";

        tutorialSetup(blobServiceClient, bucket);

        System.out.println("Uploading object...");

        URL url = new URL(FILE_URL);
        byte[] output = downloadUrl(url);

        uploadObject(blobServiceClient, bucket, key, output);

        System.out.println("Upload complete");
        System.out.printf("%n");

        cleanUp(blobServiceClient, bucket, key);
        System.out.println("Exiting...");
    }

    public static void uploadObject(BlobServiceClient blobServiceClient, String bucketName, String objectName, byte[] content) throws IOException {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucketName);

        BlobClient clientBlobClient = containerClient.getBlobClient(objectName);
        clientBlobClient.upload(BinaryData.fromBytes(content));
        System.out.println("Content uploaded to bucket " + bucketName + " as " + objectName);
    }

    public static void tutorialSetup(BlobServiceClient blobServiceClient, String bucketName) {

        System.out.println("Creating bucket: " + bucketName);

        // Instantiate blb container client
        BlobContainerClient blobContainerClient = blobServiceClient.createBlobContainer(bucketName);

        System.out.println(bucketName + " is ready.");
        System.out.printf("%n");

    }

    public static void cleanUp(BlobServiceClient blobServiceClient, String bucketName, String keyName) {
        System.out.println("Cleaning up...");

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucketName);
        System.out.println("Deleting object: " + keyName);
        containerClient.getBlobClient(keyName).delete();
        System.out.println(keyName + " has been deleted. ");
        System.out.println("Deleting bucket: " + bucketName);

        containerClient.delete();
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