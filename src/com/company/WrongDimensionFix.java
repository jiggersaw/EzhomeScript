package com.company;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.MessageFormat;
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

    private static String s3_host = "juran-prod-contents.s3.cn-north-1.amazonaws.com.cn";

    private Map<String, Float[]> modelDims = new Hashtable();
    private List<String> modelList = new ArrayList();
    private File failedLog;
    private File filesToUpdate;

    private static WrongDimensionFix getInstance(String configPath) throws IOException {
        if(fix == null) {
            fix = new WrongDimensionFix();
            fix.loadConfig(configPath);
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
            throw new InvalidPropertiesFormatException("Missing key FAILED_JSON_FILES");
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

    public void initModelDimensionMap() throws IOException {
        String tenant = "ezhome";
        List<String> ids = FileUtil.readFileAsLines(new File("C:\\color_test_data\\wrong_floor.txt"));
        String[] urls = new String[ids.size()];
        for(int i=0; i<urls.length; i++) {
            urls[i] = MessageFormat.format(GET_PROD_REST, new String[]{ids.get(i), tenant});
        }
        String[] resps = RestCallUtil.batchRestCall(urls, null, tenant, "GET");
        System.out.println("resps length ----> " + resps.length);
        for(int j = 0; j<resps.length; j++) {
            Object[] resp = JsonWorker.getDimFromContent(resps[j]);
            Float[] dim = new Float[3];
            for(int k=1; k<resp.length; k++) {
                BigDecimal bd = new BigDecimal((Float) resp[k] / 100);
                bd = bd.setScale(3, BigDecimal.ROUND_HALF_UP);
                dim[k-1] = bd.floatValue();
            }
            modelDims.put((String)resp[0], dim);
        }
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
        } catch (JsonParseException e) {
            FileUtil.appendToFile(failedLog, "Failed to parse and update json file ---> " + jsonKey);
        } catch (JsonMappingException e) {
            FileUtil.appendToFile(failedLog, "Failed to map json file ---> " + jsonKey);
        } catch (Exception e) {
            throw e;
        }


/*        if(updated) {
            try {
                S3Utils.uploadFileAsUTF8(FROM_BUCKET_NAME, jsonKey, newJson.toString());
                FileUtil.appendToFile(filesToUpdate, "Successfully upated json url ---> " + jsonKey);
            } catch (Exception e) {
                e.printStackTrace();
                FileUtil.appendToFile(failedLog, "Failed to upload file to S3, key -- > " + jsonKey);
            }
        }*/
    }

    public static void main(String[] args) throws IOException {
        WrongDimensionFix f = null;
        if(args.length == 1) {
            f = WrongDimensionFix.getInstance(args[0]);
        } else {
            throw new IllegalArgumentException("Must specify the color migration configration file path!");
        }
        f.initModelDimensionMap();
//        List<String> jsonKeys = f.getAllJsonsKeys();
        ColorMigrationWorker w = new ColorMigrationWorker();
        String tenant = "ezhome";

        List<String> jsonUrls = w.fetchS3JsonListFromDB(tenant, s3_host);
        for(String key : jsonUrls) {
            String keyName = JsonWorker.extractKeyFromUrl(key);
            try {
                f.getAndUpdate(keyName);
            } catch (JsonDataMD5CheckException e) {
                e.printStackTrace();
            } catch (JsonParseUpdateException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Dimension fix done");
    }
}
