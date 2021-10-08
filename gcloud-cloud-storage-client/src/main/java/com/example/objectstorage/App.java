package com.example.objectstorage;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;


public class App {
    public static final String FILE_URL = "https://objectcomputing.com/files/2716/2256/3799/micronaut_stacked_black.png";

    public static void main(String[] args) throws IOException {
        String projectId = System.getProperty("GCLOUD_PROJECT_ID");

        // Instantiates a client
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        String bucket = "bucket" + System.currentTimeMillis();
        String key = "micronaut-logo";

        tutorialSetup(storage, bucket);

        System.out.println("Uploading object...");

        URL url = new URL(FILE_URL);
        byte[] output = downloadUrl(url);

        uploadObject(storage, bucket, key, output);

        System.out.println("Upload complete");
        System.out.printf("%n");

        cleanUp(storage, bucket, key);
        System.out.println("Exiting...");
    }

    public static void uploadObject(Storage storage, String bucketName, String objectName, byte[] content) throws IOException {
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, content);

        System.out.println("Content uploaded to bucket " + bucketName + " as " + objectName);
    }

    public static void tutorialSetup(Storage storage, String bucketName) {

        // Creates the new bucket
        String location = "EUROPE-CENTRAL2";
        StorageClass storageClass = StorageClass.STANDARD;

        System.out.println("Creating bucket: " + bucketName);
        Bucket bucket =
                storage.create(
                        BucketInfo.newBuilder(bucketName)
                                .setStorageClass(storageClass)
                                .setLocation(location)
                                .build());

        System.out.println(bucketName + " is ready.");
        System.out.printf("%n");

    }

    public static void cleanUp(Storage storage, String bucketName, String keyName) {
        System.out.println("Cleaning up...");

        System.out.println("Deleting object: " + keyName);
        boolean deleted = storage.delete(BlobId.of(bucketName, keyName));
        System.out.println(keyName + " has been deleted. " + deleted);
        System.out.println("Deleting bucket: " + bucketName);

        Bucket bucket = storage.get(bucketName);
        bucket.delete();
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