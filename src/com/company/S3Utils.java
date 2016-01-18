package com.company;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.Md5Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by yangg on 2015/12/23.
 */
public class S3Utils {

    private static AWSCredentials credentials = null;

    private static String my_bucket_name = "george-test-bucket";

    private static AmazonS3 s3 = null;

    static {
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.", e);
        }
        s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.CN_NORTH_1);
        s3.setRegion(usWest2);
    }

    public static String getObjectMD5(String bucket, String key) {
        GetObjectMetadataRequest or = new GetObjectMetadataRequest(bucket, key);
        ObjectMetadata meta = s3.getObjectMetadata(or);
        return meta.getETag();
//        return meta.getContentMD5();
    }

    public static void uploadFileAsUTF8(String bucketName, String mirrorBucketName, String key, String content) throws Exception {
        byte[] jsonByte = new byte[0];
        try {
            jsonByte = content.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw e;
        }
        String md5BeforeUpload = Md5Utils.md5AsBase64(jsonByte);
        ByteArrayInputStream bi = new ByteArrayInputStream(jsonByte);
        uploadFile(bucketName, mirrorBucketName, key, md5BeforeUpload, bi, jsonByte.length);
    }

    public static boolean updateFileMeta(String bucketName, String key) {
        GetObjectMetadataRequest gm = new GetObjectMetadataRequest(bucketName, key);
        ObjectMetadata m =s3.getObjectMetadata(gm);
        String t = m.getContentType();
        String contentEncoding = m.getContentEncoding();
        if("application/octet-stream".equalsIgnoreCase(t) || "UTF-8".equalsIgnoreCase(contentEncoding)) {
            System.out.println("Found 1 wrong design with header ---> " + t + " content encoding: " + contentEncoding + ", key: " + key);
            GetObjectRequest gr = new GetObjectRequest(bucketName, key);
            S3Object o = s3.getObject(gr);
            ObjectMetadata m1 = new ObjectMetadata();
            m1.setContentType("application/json;charset=UTF-8");
            m1.setContentLength(m.getContentLength());
            PutObjectRequest pr = new PutObjectRequest(bucketName, key, o.getObjectContent(), m1);
            pr.withCannedAcl(CannedAccessControlList.PublicRead);
            s3.putObject(pr);
            return true;
        }
        return false;
    }

    public static void updateFilesMeta(String bucketName) {
        List<S3ObjectSummary> s3Objects = getJsonFilesFromBucket(bucketName);
        AtomicInteger cnt = new AtomicInteger(0);
        s3Objects.stream().parallel().forEach(s -> {
            if(updateFileMeta(bucketName, s.getKey())) {
                cnt.getAndIncrement();
            }
        });
        System.out.println("Found " + cnt.intValue() + " wrong content type design. Bucket: " + bucketName);
    }

    public static void uploadFile(String bucketName, String mirrorBucket, String key, String inMD5, InputStream in, long streamLen) throws Exception {
        try {
            if (!s3.doesBucketExist(bucketName)) {
                s3.createBucket(bucketName);
                System.out.println("Listing buckets");
                for (Bucket bucket : s3.listBuckets()) {
                    System.out.println(" - " + bucket.getName());
                }
            }
            System.out.println("Uploading a new object to S3 from a file...");
            ObjectMetadata meta = new ObjectMetadata();
//            meta.setContentType("application/json");
            meta.setContentEncoding("UTF-8");
            meta.setContentType("application/json");
            meta.setContentMD5(inMD5);
            meta.setContentLength(streamLen);
            PutObjectRequest pr = new PutObjectRequest(bucketName, key, in, meta);
            pr.withCannedAcl(CannedAccessControlList.PublicRead);
            PutObjectResult r = s3.putObject(pr);
            String md5 = r.getContentMd5();
            System.out.println(md5);

            if(mirrorBucket != null) {
//                GetObjectRequest gr = new GetObjectRequest(bucketName, key);
//                S3Object uploaded = s3.getObject(gr);
                if (!s3.doesBucketExist(mirrorBucket)) {
                    s3.createBucket(mirrorBucket);
                }
                CopyObjectRequest cr = new CopyObjectRequest(bucketName, key, mirrorBucket, key);
                cr.withCannedAccessControlList(CannedAccessControlList.PublicRead);
                s3.copyObject(cr);
                System.out.println("Copied: " + key + " from " + bucketName + " to " + mirrorBucket);
            }
            /*ObjectListing files = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
            List<S3ObjectSummary> objs = getJsonFilesFromBucket(bucketName);
            for(S3ObjectSummary o : objs) {
                System.out.println("Object in bucket: " + o.getKey());
                System.out.println("Object storage class in bucket: " + o.getStorageClass());
            }
            System.out.println("Retrun result MD5 ---> " + r.getContentMd5());
            System.out.println("Retrun result ETag ---> " + r.getETag());
            System.out.println("Retrun result VersionId ---> " + r.getVersionId());*/
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            throw new Exception(ase);
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            throw new Exception(ace);
        }
    }

    public static String getJsonData(String bucket, String key) {
        GetObjectRequest gr = new GetObjectRequest(bucket, key);
        S3Object o = s3.getObject(gr);
        BufferedReader reader = new BufferedReader(new InputStreamReader(o.getObjectContent()));
        StringBuilder buf = new StringBuilder();
        while(true) {
            String line = null;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(line == null) break;
            buf.append(line);
        }
        if(reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return buf.toString();
    }

    public static void uploadFile(String bucketName, String mirrorBucketName, String key, String md5, File file) throws Exception {
        uploadFile(bucketName, mirrorBucketName, key, md5, new FileInputStream(file), file.length());
    }

    public static void delFile(String bucketName, String key) {
        if (!s3.doesBucketExist(bucketName)) {
            System.err.println("Bucket " + bucketName + " does not exist.");
            return;
        }
        s3.deleteObject(bucketName, key);
    }

    public static List<String> getJsonFiles(String bucket) {
        List<String> keys = new ArrayList();
        ObjectListing objectListing = s3.listObjects(bucket);
        while (true) {
            for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
                keys.add(objectSummary.getKey());
            }
            if (objectListing.isTruncated()) {
                objectListing = s3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
        return keys.stream().filter(s -> s.endsWith(".json")).collect(Collectors.toList());
    }

    public static void delBucket(String bucketName, boolean force) {
        if(s3.doesBucketExist(bucketName)) {
            ObjectListing objectListing = s3.listObjects(bucketName);
            long delCnt=0;
            while (true) {
                for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                    S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
                    s3.deleteObject(bucketName, objectSummary.getKey());
                    System.out.println("Delete key: " + objectSummary.getKey());
                    delCnt++;
                }
                if (objectListing.isTruncated()) {
                    objectListing = s3.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            if (force) {
                s3.deleteBucket(bucketName);
            }
            System.out.println("Totally removed " + delCnt + " files.");
        }
    }

    public static List<S3ObjectSummary> getJsonFilesFromBucket(String bucket) {
        ListObjectsRequest r = new ListObjectsRequest().withBucketName(bucket);
//        r.setSdkRequestTimeout(1000*180);
//        r.setSdkClientExecutionTimeout(1000*180);
        List<S3ObjectSummary> allObjs = new ArrayList();

        ObjectListing objects = s3.listObjects(r);
        do {
//            objects = s3.listObjects(r);
            allObjs.addAll(objects.getObjectSummaries().stream().filter(x -> x.getKey().endsWith(".json")).collect(Collectors.toList()));
            objects = s3.listNextBatchOfObjects(objects);
        } while(objects.isTruncated());
        allObjs.addAll(objects.getObjectSummaries().stream().filter(x -> x.getKey().endsWith(".json")).collect(Collectors.toList()));
        return allObjs;
    }



    public static void copyJsonFiles(String srcBucket, String desBucket, String bucketName) {
        List<S3ObjectSummary> srcObjs = getJsonFilesFromBucket(srcBucket);
        if (!s3.doesBucketExist(desBucket)) {
            s3.createBucket(desBucket);
        }
        srcObjs.stream().parallel().forEach(o -> {
            System.out.println(o.getKey());
            if (o.getBucketName().contains(bucketName)) {
                CopyObjectRequest cr = new CopyObjectRequest(srcBucket, o.getKey(), desBucket, o.getKey());
                cr.withCannedAccessControlList(CannedAccessControlList.PublicRead);
                s3.copyObject(cr);
            }
        });
/*        for (S3ObjectSummary o : srcObjs) {
            System.out.println(o.getKey());
            if (o.getBucketName().contains(bucketName)) {
                CopyObjectRequest cr = new CopyObjectRequest(srcBucket, o.getKey(), desBucket, o.getKey());
                cr.withCannedAccessControlList(CannedAccessControlList.PublicRead);
                s3.copyObject(cr);
            }
        }*/
    }

    public static String backupFile(String srcBucket, String srcKeyName, String destBucket) throws Exception {
//        List<S3ObjectSummary> srcObjs = getJsonFilesFromBucket(srcBucket);
        try {
            if (!s3.doesBucketExist(destBucket)) {
                s3.createBucket(destBucket);
            }
            System.out.println("Backup " + srcKeyName + " from " + srcBucket + " to bucket " + destBucket);
//            AccessControlList acl = s3.getObjectAcl(srcBucket, srcKeyName);
            CopyObjectRequest cr = new CopyObjectRequest(srcBucket, srcKeyName, destBucket, srcKeyName);
            cr.withCannedAccessControlList(CannedAccessControlList.PublicRead);
//            cr.setAccessControlList(acl);
            s3.copyObject(cr);
        } catch (AmazonServiceException e1) {
            e1.printStackTrace();
            throw new Exception(e1);
        } catch (AmazonClientException e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        return srcKeyName;
    }

    public static void updatePermission(String bucketName, String fileName) {
//        List<S3ObjectSummary> allJsons = getJsonFilesFromBucket(bucketName);
        List<String> urls = null;
        try {
            urls = ColorMigrationWorker.fetchS3JsonListFromFile(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long cnt = 0;
        System.out.println("Starting to update permissions...");
        for(String u : urls) {
            String keyName = JsonWorker.extractKeyFromUrl(u);
            s3.setObjectAcl(new SetObjectAclRequest(bucketName, keyName, CannedAccessControlList.PublicRead));
            cnt++;
            System.out.println("Updated " + cnt + " records.");
        }
/*        Statement allowPublicReadStatement = new Statement(Statement.Effect.Allow)
                .withPrincipals(Principal.AllUsers)
                .withActions(S3Actions.GetObject)
                .withResources(new S3ObjectResource(bucketName, fileType));
        Policy policy = new Policy().withStatements(allowPublicReadStatement);*/
        System.out.println("Update permission, DONE!");
//        s3.setBucketPolicy(bucketName, policy.toJson());
    }

    public static void main(String[] args) throws Exception {
//        delBucket("juran-prod-contents-george", false);
//        uploadFile(my_bucket_name, "george_test_file_key", new File("c:\\s3_resp.txt"));
//        copyJsonFiles("juran-staging-contents", "juran-staging-contents", "juran-staging-contents");
//        copyJsonFiles("juran-prod-contents-george", "juran-prod-contents-color-test", "juran-prod-contents-george");
//        String jsonUrlFile = "C:\\color_test_data\\files_to_update2.txt";
//        updatePermission("juran-staging-contents", jsonUrlFile);
        String bucketName = args[0];
        updateFilesMeta(bucketName);
    }

}
