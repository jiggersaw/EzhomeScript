package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by yangg on 2016/1/7.
 */
public class WrongDimensionFix {

    private static WrongDimensionFix fix;

    private String REST_HOST;
    private String GET_PROD_BY_ID;
    private String PORT;

    private String GET_PROD_REST;
    private String MODEL_LIST_FILE;
    private String FROM_BUCKET_NAME;
    private static String FAILED_JSON_FILES;
    private static String FILES_TO_BE_UPDATE;
    private static String JSON_FILE_URL;


    private Map<String, Float[]> modelDims = new HashMap();
    private List<String> modelList = new ArrayList();
    private File failedLog;
    private File filesToUpdate;

    private static WrongDimensionFix getInstance(String configPath) {
        if(fix == null) {
            fix = new WrongDimensionFix();
            fix.failedLog = new File(FAILED_JSON_FILES);
            fix.filesToUpdate = new File(FILES_TO_BE_UPDATE);
        }
        return fix;
    }

    private void loadConfig(String configPath) throws IOException {
        Properties p = new Properties();
        p.load(new FileReader(new File(configPath)));
        if((REST_HOST = p.getProperty("REST_HOST")) == null) throw new InvalidPropertiesFormatException("Missing REST_HOST");
        if((GET_PROD_BY_ID = p.getProperty("GET_PROD_BY_ID")) == null) throw new InvalidPropertiesFormatException("Missing GET_PROD_BY_ID");
        if((PORT = p.getProperty("PORT")) == null) throw new InvalidPropertiesFormatException("Missing PORT");
        if((GET_PROD_REST = p.getProperty("REST_GET_PROD_URL")) == null) throw new InvalidPropertiesFormatException("Missing GET_PROD_REST");
        if((MODEL_LIST_FILE = p.getProperty("MODEL_LIST_FILE")) == null) throw new InvalidPropertiesFormatException("Missing MODEL_LIST_FILE");
        if((FROM_BUCKET_NAME = p.getProperty("FROM_BUCKET_NAME")) == null) throw new InvalidPropertiesFormatException("Missing FROM_BUCKET_NAME");
        if((FAILED_JSON_FILES = p.getProperty("FAILED_JSON_FILES")) == null) {
            throw new InvalidPropertiesFormatException("Missing key AWS_SECRET_KEY");
        } else {
            FAILED_JSON_FILES = FAILED_JSON_FILES + "_" + FROM_BUCKET_NAME;
        }
        if((FILES_TO_BE_UPDATE = p.getProperty("FILES_TO_BE_UPDATE")) == null) {
            throw new InvalidPropertiesFormatException("Missing key FILES_TO_BE_UPDATE");
        } else {
            FILES_TO_BE_UPDATE = FILES_TO_BE_UPDATE + "_" + FROM_BUCKET_NAME;
        }
        if((JSON_FILE_URL = p.getProperty("JSON_FILE_URL")) == null) {
            throw new InvalidPropertiesFormatException("Missing key JSON_FILE_URL");
        } else {
            JSON_FILE_URL = JSON_FILE_URL + "_" + FROM_BUCKET_NAME;
        }
        modelList = FileUtil.readFileAsLines(new File(MODEL_LIST_FILE));
    }

    public List<String> getAllJsonsKeys() {
        return S3Utils.getJsonFiles(FROM_BUCKET_NAME);
    }

    public void getAndUpdate(String jsonKey) throws JsonDataMD5CheckException,
            JsonParseUpdateException {
        String s3Key = jsonKey;
        String md5OnS3;
        try {
            md5OnS3 = S3Utils.getObjectMD5(FROM_BUCKET_NAME, s3Key);
        } catch (Exception e) {
            FileUtil.appendToFile(failedLog, "Failed to get MD5 for -- > " + jsonKey);
            throw e;
        }
        String jsonData = JsonWorker.tryReadCorrectJson(FROM_BUCKET_NAME, jsonKey, md5OnS3, failedLog);
        if (md5OnS3 == null) {
            System.out.println("Failed to get MD5 for -- > " + jsonKey);
            FileUtil.appendToFile(failedLog, "Failed to get MD5 for -- > " + jsonKey);
            throw new JsonDataMD5CheckException("Failed to get MD5 for -- > " + jsonKey);
        } else if (jsonData == null) {
            System.out.println("MD5 not match after retrieving from ---->" + jsonKey);
            throw new JsonDataMD5CheckException("MD5 not match after retrieving from ---->" + jsonKey);
        }
        StringBuilder newJson = new StringBuilder();
        boolean updated = true;
        try {
            updated = JsonWorker.updateContentDimension(jsonData, modelDims, newJson);
        } catch (Exception e) {
            FileUtil.appendToFile(failedLog, "Failed to parse and update json file ---> " + jsonKey);
            throw new JsonParseUpdateException("Failed to parse and update json file ---> " + jsonKey);
        }
        if(updated) {
            try {
                S3Utils.uploadFileAsUTF8(FROM_BUCKET_NAME, jsonKey, newJson.toString());
                FileUtil.appendToFile(filesToUpdate, "Successfully upated json url ---> " + jsonKey);
            } catch (Exception e) {
                e.printStackTrace();
                FileUtil.appendToFile(failedLog, "Failed to upload file to S3, key -- > " + jsonKey);
            }
        }
    }

    public static void main(String[] args) {
        WrongDimensionFix f = null;
        if(args.length == 1) {
            f = WrongDimensionFix.getInstance(args[0]);
        } else {
            throw new IllegalArgumentException("Must specify the color migration configration file path!");
        }
        List<String> jsonKeys = f.getAllJsonsKeys();
        for(String key : jsonKeys) {
            try {
                f.getAndUpdate(key);
            } catch (JsonDataMD5CheckException e) {
                e.printStackTrace();
            } catch (JsonParseUpdateException e) {
                e.printStackTrace();
            }
        }
        //1.Call catalog service to initialize model dimension map

    }
}
