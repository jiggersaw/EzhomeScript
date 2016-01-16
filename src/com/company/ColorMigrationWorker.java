package com.company;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.Md5Utils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yangg on 2015/12/21.
 */
public class ColorMigrationWorker {

//    private static final String S3_HOST = "hs-backend-content-dev.s3.amazonaws.com";
//    private static final String DB_URL = "jdbc:mysql://hsmdb.alpha.homestyler.com/hscontent?characterEncoding=UTF-8";
//    private static String DB_URL = "jdbc:mysql://home-content-dev-cn-1.cfyuzargym1l.rds.cn-north-1.amazonaws.com.cn/hscontent?characterEncoding=UTF-8";
    private static String DB_URL;
//    private static final String usrName = "hsmroot";
//    private static final String pwd = "hsm123**";

    private static String usrName;
    private static String pwd;

    private static String AWS_ACCESS_KEY = "AKIAPVHMQY6BUZ56H2MQ";
    private static String AWS_SECRET_KEY = "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";

    private static String COLOR_MAP_FILE = "C:\\color_test_data\\color_map_ezhome.txt";

    private static String FAILED_JSON_FILES = "C:\\color_test_data\\failed_json_upload.txt";

    private static String FILES_TO_BE_UPDATE = "C:\\color_test_data\\files_to_update.txt";

    private static String JSON_FILE_URL = "c:\\color_test_data\\json_urls.txt";

//    private static final String DEST_UPDATE_BUCKET = "george-test-bucket";

    private static String DEST_UPDATE_BUCKET;

    private static String DEST_UPDATE_BUCKET_MIRROR;

    private static String BACKUP_BUCKET = "george-backup-bucket";

    private static String colorCategoryId;

    private static String tenant;

    private static String s3_host;

    private static boolean NEED_BACKUP_BUCKET = true;

    private static String CONFIG_DIR;

    private Connection conn = null;

    private PreparedStatement pStat = null;

    public static Map<String, String[]> loadColorMap() throws IOException {
        FileReader fr = null;
        Map<String, String[]> colorMap = new HashMap();
        try {
            fr = new FileReader(new File(COLOR_MAP_FILE));
            BufferedReader bf = new BufferedReader(fr);
            String line = "";
            bf.readLine();
            while ((line = bf.readLine()) != null) {
                String[] items = line.split(",");
                if (items == null || items.length != 3)
                    throw new IllegalArgumentException("Wrong color maping file found.");
                colorMap.put(items[0], new String[] {items[1], items[2]});
            }
        } finally {
            fr.close();
        }
        return colorMap;
    }


    public void loadConfig(String configPath) throws IOException {
        Properties p = new Properties();
        p.load(new FileReader(new File(configPath)));
//        p.load(ColorMigrationWorker.class.getClassLoader().getResourceAsStream(configPath));
        if((DEST_UPDATE_BUCKET = p.getProperty("DEST_UPDATE_BUCKET")) == null) throw new InvalidPropertiesFormatException("Missing key DEST_UPDATE_BUCKET");
        if((DEST_UPDATE_BUCKET_MIRROR = p.getProperty("DEST_UPDATE_BUCKET_MIRROR")) == null) throw new InvalidPropertiesFormatException("Missing key DEST_UPDATE_BUCKET_MIRROR");
        if((COLOR_MAP_FILE = p.getProperty("COLOR_MAP_FILE")) == null) throw new InvalidPropertiesFormatException("Missing key COLOR_MAP_FILE");
        if((AWS_ACCESS_KEY = p.getProperty("AWS_ACCESS_KEY")) == null) throw new InvalidPropertiesFormatException("Missing key AWS_ACCESS_KEY");
        if((AWS_SECRET_KEY = p.getProperty("AWS_SECRET_KEY")) == null) throw new InvalidPropertiesFormatException("Missing key AWS_SECRET_KEY");
        if((FAILED_JSON_FILES = p.getProperty("FAILED_JSON_FILES")) == null) {
            throw new InvalidPropertiesFormatException("Missing key AWS_SECRET_KEY");
        } else {
            FAILED_JSON_FILES = FAILED_JSON_FILES + "_" + DEST_UPDATE_BUCKET;
        }
        if((FILES_TO_BE_UPDATE = p.getProperty("FILES_TO_BE_UPDATE")) == null) {
            throw new InvalidPropertiesFormatException("Missing key FILES_TO_BE_UPDATE");
        } else {
            FILES_TO_BE_UPDATE = FILES_TO_BE_UPDATE + "_" + DEST_UPDATE_BUCKET;
        }
        if((JSON_FILE_URL = p.getProperty("JSON_FILE_URL")) == null) {
            throw new InvalidPropertiesFormatException("Missing key JSON_FILE_URL");
        } else {
            JSON_FILE_URL = JSON_FILE_URL + "_" + DEST_UPDATE_BUCKET;
        }
        if((BACKUP_BUCKET = p.getProperty("BACKUP_BUCKET")) == null) throw new InvalidPropertiesFormatException("Missing key BACKUP_BUCKET");
        if((usrName = p.getProperty("usrName")) == null) throw new InvalidPropertiesFormatException("Missing key usrName");
        if((pwd = p.getProperty("pwd")) == null) throw new InvalidPropertiesFormatException("Missing key pwd");
        if((DB_URL = p.getProperty("DB_URL")) == null) throw new InvalidPropertiesFormatException("Missing key DB_URL");
        if((colorCategoryId = p.getProperty("colorCategoryId")) == null) throw new InvalidPropertiesFormatException("Missing key colorCategoryId");
        if((tenant = p.getProperty("tenant")) == null) throw new InvalidPropertiesFormatException("Missing key tenant");
        if((s3_host = p.getProperty("s3_host")) == null) throw new InvalidPropertiesFormatException("Missing key s3_host");
        if(p.getProperty("NEED_BACKUP_BUCKET") == null) throw new InvalidPropertiesFormatException("NEED_BACKUP_BUCKET");
        else NEED_BACKUP_BUCKET = Boolean.valueOf(p.getProperty("NEED_BACKUP_BUCKET"));
    }

    private void initMySqlConn() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch(ClassNotFoundException e){
            e.printStackTrace();
            return;
        }
        try {
            System.out.println("DB_URL ---> " + DB_URL);
            conn = DriverManager.getConnection(DB_URL, usrName, pwd);
        }catch(SQLException se) {
            se.printStackTrace();
        }
//        conn.prepareStatement()
    }

    private void releaseMysqlConn(Statement stat, Connection conn) {
        try {
            stat.close();
        } catch (SQLException e) {
            e.printStackTrace();
            stat = null;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            stat = null;
        }
    }

    private static void toFile(String content, String fileName) throws IOException {
        BufferedWriter bf = new BufferedWriter(new FileWriter(fileName));
        bf.write(content);
        bf.close();
    }

    public static List<String> fetchS3JsonListFromFile(String fileName) throws IOException {
        List<String> lines = FileUtil.readFileAsLines(new File(fileName));
        return lines;
    }

    public List<String> fetchS3JsonListFromDB(String tenant, String hostName) {
        System.out.println("Starting in fetchS3JsonListFromDB...");
        List<String> jsonDataList = new ArrayList();
        if(conn == null) {
            initMySqlConn();
        }
        String sql = "SELECT a.tenant, a.Data FROM hscontent.Assets a where tenant=?";
        pStat = null;
        File f = new File(JSON_FILE_URL);
        BufferedWriter bof = null;
        try {
            bof = new BufferedWriter(new FileWriter(f));
            try {
                pStat = conn.prepareStatement(sql);
                System.out.println("tenant ---> " + tenant);
                pStat.setString(1, tenant);
                ResultSet rs = pStat.executeQuery();
                while (rs.next()) {
                    String d = rs.getString(2);
                    if (d.startsWith("'") && d.endsWith("'")) {
                        d = d.substring(1, d.length() - 2);
                    }
                    jsonDataList.add(d);
//                    System.out.println(d);
                    bof.write(d);
                    bof.write("\r\n");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                releaseMysqlConn(pStat, conn);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(bof != null) {
                try {
                    bof.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    bof = null;
                }
            }
        }
        return jsonDataList.stream().filter(l -> (l.indexOf(hostName) > 0)).collect(Collectors.toList());
    }

    public void uploadFileToS3(File fileName) {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format." ,e);
        }
        //http://juran-staging-contents.s3.cn-north-1.amazonaws.com.cn/Asset/0006db23-4195-4a94-a68a-f0e59d4f49fd/6a97877e-3d0c-4f63-8ea4-f61926372b82.json
        try {
            AmazonS3 s3 = new AmazonS3Client(credentials);
            Region usWest2 = Region.getRegion(Regions.CN_NORTH_1);
            s3.setRegion(usWest2);

//            String bucketName = "juran-staging-contents";
            String bucketName = "george_test_bucket";
//            s3.createBucket(bucketName);
//            System.out.println("Listing buckets");
//            for (Bucket bucket : s3.listBuckets()) {
//                System.out.println(" - " + bucket.getName());
//            }

            System.out.println("Uploading a new object to S3 from a file\n");
            PutObjectResult r = s3.putObject(new PutObjectRequest(bucketName, "", fileName));
            System.out.println("Retrun result MD5 ---> " + r.getContentMd5());
            System.out.println("Retrun result ETag ---> " + r.getETag());
            System.out.println("Retrun result VersionId ---> " + r.getVersionId());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

    }

    public String retrieveJson(String url) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        BufferedReader bf = null;
        StringBuilder r = new StringBuilder();
        String rs = "";
        try {
            CloseableHttpResponse resp = httpClient.execute(httpGet);
            HttpEntity entity = resp.getEntity();
            bf = new BufferedReader(new InputStreamReader(entity.getContent()));
            String l = "";
            while((l=bf.readLine()) != null) {
                r.append(l);
            }
//            char[] charArray = new char[100];
//            int num;
//            int offset = 0, len = 100;
//            while((num = bf.read(charArray, offset, len)) != -1) {
//                System.out.println(num);
//                if(num < len && num > 0) {
//                    r.append(charArray, 0, num);
////                    offset += num;
////                    len -= num;
//                } else if(num == len) {
//                    offset = 0;
//                    len = 100;
//                    r.append(charArray);
//                }
//
//                String strBuf = new String(charArray);
//                System.out.println(strBuf);
//            }
            rs = r.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bf.close();
            } catch (IOException e) {
                e.printStackTrace();
                bf = null;
            }
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
                httpClient = null;
            }
        }
        return rs;
    }

    private String tryReadCorrectJson(String bucket, String key, String md5OnS3, File failedLog) {
        boolean success = false;
        int cnt = 0;
        String md5AfterRead = "";
        String jsonData = null;
        while(!success && cnt < 3) {
//            jsonData = retrieveJson(key);
            jsonData = S3Utils.getJsonData(bucket, key);
            byte[] jsonByte0 = new byte[0];
            try {
                jsonByte0 = jsonData.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            byte[] mdBytes = Md5Utils.computeMD5Hash(jsonByte0);
            md5AfterRead = MDHexUtil.toHex(mdBytes);
            if(md5AfterRead.equalsIgnoreCase(md5OnS3)) {
                success = true;
            } else {
                jsonData = null;
                if(failedLog != null) {
                    FileUtil.appendToFile(failedLog, "MD5 not match! md5OnS3: " + md5OnS3 + " md5AfterRead: " + md5AfterRead);
                    FileUtil.appendToFile(failedLog, "Trying to read again from ----> " + key);
                }
                System.out.println("Trying to read again from ----> " + key);
                cnt++;
            }
        }
        return jsonData;
    }

    public static void main(String[] args) throws Exception {
//       String s =  retrieveJson("https://george-test-bucket.s3.cn-north-1.amazonaws.com.cn/Asset/9ef2fc06-7b37-4caa-8112-4dfe5ad0c125/6fa033db-9409-4fe1-97bc-298db16020fc.json");
//        FileUtil.writeToFile(new File("C:\\color_test_data\\encode_issue.json"), s);
//        NEED_BACKUP_BUCKET = false;
//        String colorCategoryId = "d72316f9-ed94-4872-9663-206471783a18";

        ColorMigrationWorker w = new ColorMigrationWorker();
        int updateCnt = 0;
        if(args.length == 1) {
            w.loadConfig(args[0]);
        } else {
            throw new IllegalArgumentException("Must specify the color migration configration file path!");
        }
        System.out.println("Start to call fetchS3JsonListFromDB, tenant " + tenant + ", s3_host: " + s3_host);
        List<String> jsonList = w.fetchS3JsonListFromDB(tenant, s3_host);
//        List<String> jsonList = S3Utils.getJsonFiles(DEST_UPDATE_BUCKET);
//        List<String> jsonList = w.fetchS3JsonListFromFile("C:\\color_test_data\\files_to_update2.txt");
        File jsonUrlFile = new File(JSON_FILE_URL);
        for(String s: jsonList) {
            FileUtil.appendToFile(jsonUrlFile, s);
        }
        FileUtil.finishAppend(jsonUrlFile);
        Map<String, String[]> colorMap = w.loadColorMap();
        File failedLog = new File(FAILED_JSON_FILES);
        File filesToUpdate = new File(FILES_TO_BE_UPDATE);
        long l = 0;
        if(jsonList.size() == 0) {
            FileUtil.appendToFile(failedLog, "No json files found in Mysql DB.");
            System.out.println("No json files found in Mysql DB.");
            System.exit(1);
        }
        for(String jsonUrl : jsonList) {
            String keyName = JsonWorker.extractKeyFromUrl(jsonUrl);
//            String keyName = key;
            if(keyName == null){
                FileUtil.appendToFile(failedLog, "Wrong json url found: " + jsonUrl);
                continue;
            }
            // 2. Read each json file from s3, parse and update it.
            System.out.println("Reading " + jsonUrl);

//            String jsonData = w.retrieveJson(url);
//            byte[] jsonByte0 = jsonData.getBytes("UTF-8");

//            String s3Key = JsonWorker.extractKeyFromUrl(key);
            String s3Key = keyName;
            String md5OnS3;
            try {
                md5OnS3 = S3Utils.getObjectMD5(DEST_UPDATE_BUCKET, s3Key);
            } catch (Exception e) {
                FileUtil.appendToFile(failedLog, "Failed to get MD5 for -- > " + jsonUrl);
                continue;
            }
//            byte[] mdBytes = Md5Utils.computeMD5Hash(jsonByte0);
//            String md5AfterRead = MDHexUtil.toHex(mdBytes);
//            String md5AfterRead = Md5Utils.md5AsBase64(jsonByte0);
            String jsonData = w.tryReadCorrectJson(DEST_UPDATE_BUCKET, keyName, md5OnS3, failedLog);
            if (md5OnS3 == null) {
                System.out.println("Failed to get MD5 for -- > " + jsonUrl);
                FileUtil.appendToFile(failedLog, "Failed to get MD5 for -- > " + jsonUrl);
                continue;
            } else if (jsonData == null) {
                System.out.println("MD5 not match after retrieving from ---->" + jsonUrl);
//                System.out.println("md5OnS3: " + md5OnS3 + " md5AfterRead: " + md5AfterRead);
//                FileUtil.appendToFile(failedLog, "MD5 not match after retrieving from ---->" + url);
//                FileUtil.appendToFile(failedLog, "md5OnS3: " + md5OnS3 + " md5AfterRead: " + md5AfterRead);
                continue;
            }

            StringBuilder newJson = new StringBuilder();
            boolean updated = true;
            try {
                updated = JsonWorker.updateWallPaint(jsonData, colorMap, colorCategoryId, newJson);
            } catch (Exception e) {
//                e.printStackTrace();
                FileUtil.appendToFile(failedLog, "Failed to parse and update json file ---> " + jsonUrl);
                continue;
            }

            // 3. Replace the json file with the updated one
            if(updated) {
//                FileUtil.appendToFile(filesToUpdate, "Trying to update json url ---> " + url);
                //Backup to S3
                if(NEED_BACKUP_BUCKET) {
                    updateCnt++;
                    try {
//                        S3Utils.delBucket(BACKUP_BUCKET, false);
                        S3Utils.backupFile(DEST_UPDATE_BUCKET, keyName, BACKUP_BUCKET);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        FileUtil.appendToFile(failedLog, "Failed to backup " + keyName + " to " + BACKUP_BUCKET);
                        continue;
                    }
                }
                //Write filename to local
//                FileUtil.writeToFile(new File(LOCAL_TMP_FOLDER + "\\" + jsonFileName), newJson.toString());
//                FileUtil.writeToFile(new File(LOCAL_TMP_FOLDER + "\\" + jsonFileName), jsonData);
                byte[] jsonByte = new byte[0];
                try {
                    jsonByte = newJson.toString().getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    continue;
                }
                String md5BeforeUpload = Md5Utils.md5AsBase64(jsonByte);
                ByteArrayInputStream bi = new ByteArrayInputStream(jsonByte);
                // Update file on s3
                try {
                    S3Utils.uploadFile(DEST_UPDATE_BUCKET, DEST_UPDATE_BUCKET_MIRROR, keyName, md5BeforeUpload, bi, jsonByte.length);
                    FileUtil.appendToFile(filesToUpdate, "Successfully upated json url ---> " + jsonUrl);
                } catch (Exception e) {
                    e.printStackTrace();
                    FileUtil.appendToFile(failedLog, "Failed to upload file to S3, key -- > " + keyName);
                    continue;
                }
            }
            // 4. If any error during reading, parsing, updating or uploading, then continue to process next file and write the failed
            // file name to some log file.
            l++;
            System.out.println("Processed " + l + " files...");
          }
        System.out.println("Found " + updateCnt + " json files need to be updated.");
        FileUtil.finishAppend(failedLog);
        FileUtil.finishAppend(filesToUpdate);
        System.out.println("Migration done on bucket " + DEST_UPDATE_BUCKET);



       /* ColorMigrationWorker w = new ColorMigrationWorker();
        if(args.length == 1) {
            w.loadConfig(args[0]);
        } else {
            throw new IllegalArgumentException("Must specify the color migration configration file path!");
        }
//        String jsonData = w.retrieveJson("https://george-test-bucket.s3.cn-north-1.amazonaws.com.cn/Asset/9ef2fc06-7b37-4caa-8112-4dfe5ad0c125/6fa033db-9409-4fe1-97bc-298db16020fc.json");
        String jsonUrl = "https://juran-prod-contents.s3.cn-north-1.amazonaws.com.cn/Asset/e39ba5c5-a6e7-4917-b4c3-fb69a07f6368/6cb37b59-50df-46d2-bd4f-07537152256d.json";

//        String jsonData = w.retrieveJson(jsonUrl);
        String s3Key = JsonWorker.extractKeyFromUrl(jsonUrl);
        String md5OnS3 = S3Utils.getObjectMD5("juran-prod-contents", s3Key);
        String jsonData = w.tryReadCorrectJson("juran-prod-contents", s3Key, md5OnS3, null);
        byte[] jsonByte = jsonData.getBytes("UTF-8");

//        String jsonData = w.tryReadCorrectJson("george-test-bucket", s3Key, md5OnS3, null);
//        String md5AfterRead = Md5Utils.md5AsBase64(jsonByte);
        String md5AfterRead = MDHexUtil.getMD5Hex(jsonByte);
        if (md5OnS3 == null || md5AfterRead == null) throw new Exception("Failed to get MD5 ");
        else if (!md5AfterRead.equalsIgnoreCase(md5OnS3)) throw new Exception("MD5 not same for md5AfterRead, md5OnS3");

        StringBuilder ss = new StringBuilder();
        try {
            JsonWorker.updateWallPaint(jsonData, ColorMigrationWorker.loadColorMap(), colorCategoryId, ss);
            jsonByte = new byte[0];
            try {
                jsonByte = ss.toString().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            ByteArrayInputStream bi = new ByteArrayInputStream(jsonByte);
            String md5 = Md5Utils.md5AsBase64(jsonByte);
            try {
                S3Utils.uploadFile("george-test-bucket", "6cb37b59-50df-46d2-bd4f-07537152256d.json", md5, bi, jsonByte.length);
            } catch (Exception e) {
                e.printStackTrace();
            }
//            FileUtil.writeToFile(new File("C:\\color_test_data\\dummy.json"), ss.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }*/

//        ObjectMapper om = new ObjectMapper();
//        Map m = om.readValue(jsonData, Map.class);
//        jsonData = om.writeValueAsString(m);
//        byte[] jsonByte1 = new byte[0];
//        try {
//            jsonByte1 = jsonData.toString().getBytes("UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//        String md5BeforeUpload = Md5Utils.md5AsBase64(jsonByte1);
//        ByteArrayInputStream bi = new ByteArrayInputStream(jsonByte1);
//        try {
//            S3Utils.uploadFile("george-test-bucket", "encoding_issue2.json", md5BeforeUpload, bi, jsonByte1.length);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        System.out.println("DONE");
    }
}



 
