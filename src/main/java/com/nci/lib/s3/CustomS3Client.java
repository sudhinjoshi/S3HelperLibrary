package com.nci.lib.s3;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;

public class CustomS3Client {

    private AmazonS3 s3;

    /**
     * Method to download AWS S3 bucket object
     * @param bucketName - BucketName
     * @param fileName - Object to be downloaded
     */
    public byte[] s3download(String bucketName, String fileName) throws IOException {

        byte[] content = null;
        System.out.println("-------------  CustomS3Client --------------");
        System.out.println("Inside s3download() bucket Name: "+bucketName+" and file Name: "+ fileName);

        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion("us-east-1")
                .build();

        // List current buckets.
        ListMyBuckets(bucketName);

        final S3Object s3Object = s3.getObject(bucketName, fileName);
        S3ObjectInputStream objectInputStream = s3Object.getObjectContent();

        System.out.println("-------------  CustomS3Client --------------");
        System.out.println("-------------       Success --------------");
        return IOUtils.toByteArray(objectInputStream);
    }

    /**
     * Method to upload AWS S3 bucket object
     * @param bucketName - BucketName
     * @param fileName - Object Key
     * @param file - File object to be uploaded
     */
    public void s3upload(String bucketName, String fileName, File file) throws IOException {

        System.out.println("-------------  CustomS3Client --------------");
        System.out.println("Inside s3upload bucket Name: "+bucketName+" and file Name: "+ fileName);

        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion("us-east-1")
                .build();

        // List current buckets.
        ListMyBuckets(bucketName);

        //upload the file
        s3.putObject(bucketName, fileName, file);

        System.out.println("-------------       Success --------------");

    }

    /**
     * Method to list AWS S3 bucket contents
     */
    private void ListMyBuckets(String bucketName) {
        List<Bucket> buckets = s3.listBuckets();
        System.out.println("-------------  CustomS3Client  --------------");
        System.out.println("Bucket "+bucketName+" contents are as below:");

        for (Bucket b : buckets) {
            System.out.println(b.getName());
            ListObjectsV2Result result = s3.listObjectsV2(b.getName());
            List<S3ObjectSummary> objects = result.getObjectSummaries();
            for (S3ObjectSummary os : objects) {
                System.out.println("** " + os.getKey());
            }
        }
        System.out.println("------------- All bucket contents listed successfully --------------");
    }

    /**
     * Method to delete AWS S3 bucket object
     * @param bucketName - BucketName
     * @param fileName - Object Key to be deleted
     */
    public void s3delete(String bucketName, String fileName) throws IOException {

        System.out.println("-------------  CustomS3Client --------------");
        System.out.println("Inside s3delete bucket Name: "+bucketName+" and file Name: "+ fileName);

        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion("us-east-1")
                .build();

        // List current buckets.
        ListMyBuckets(bucketName);

        //upload the file
        s3.deleteObject(bucketName, fileName);

        System.out.println("-------------       Success --------------");

    }

}
