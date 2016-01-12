package com.company;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.URI;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ColorUploader {

    private static long start;
    private static long end;

    public enum ARGS {
        RGB(1), GROUP(0), STYLE(2);
        private int idx;

        private ARGS(int i) {
            idx = i;
        }

        public int value() {
            return idx;
        }
    }

//    private static String csvFileName = "C:\\color_test_data\\color_small.csv";
    private static String csvFileName = "";

//    private static String successGUIDFile = "C:\\color_test_data\\guids.txt";
    private static String successGUIDFile = "";

    private static ArrayBlockingQueue<String> okQueue = new ArrayBlockingQueue(11);

    private static volatile boolean isStop = false;

    private static volatile boolean isNew = true;

    //    private static final String STYLE_NODE_ID = "9221463d-18ca-4b63-b09b-a2000585a00b";
//    private static String STYLE_NODE_ID = "9221463d-18ca-4b63-b09b-a2000585a00b";
    private static String STYLE_NODE_ID = "";

    //    private static final String CONTENT_NODE_ID = "6d6d224e-bae0-4045-8c98-b044332ee7aa";
//    private static String CONTENT_NODE_ID = "dbd9b78c-fbae-4931-ac30-ff218059b3c6";
    private static String CONTENT_NODE_ID = "";

    //    private static final String PROD_NODE_ID = "f2ebc921-291a-42c6-be8d-e6f3019672fb";
//    private static String PROD_NODE_ID = "f2ebc921-291a-42c6-be8d-e6f3019672fb";
    private static String PROD_NODE_ID = "";

//    private static String BRAND_SELECTED_ID = "e0e6cf4b-877d-4287-8060-a7c2406f84a1"; //The generic brand id
    private static String BRAND_SELECTED_ID = ""; //The generic brand id

    //    private static final String RETAIL_SELECTED_ID = "de9d4c65-f747-4963-acfa-cea44c475fcc";
//    private static String RETAIL_SELECTED_ID = "b377bedf-a540-4376-b4d2-3d4899c1885d";
    private static String RETAIL_SELECTED_ID = "";

//    private static String PAINT_PAINT = "c24ea8d7-e1d8-4b26-8ef2-7753fb21f8e2"; //paint/paint
    private static String PAINT_PAINT = ""; //paint/paint

    private static String tenant = "";

    //    private static final String CATALOG_HOST = "catalog-be.alpha.homestyler.com";
//    private static final String CATALOG_HOST = "ec2-54-223-154-174.cn-north-1.compute.amazonaws.com.cn";
//    private static String CATALOG_HOST = "ec2-54-223-150-170.cn-north-1.compute.amazonaws.com.cn";
    private static String CATALOG_HOST = "";

    private static String WARM_STYLE_VAL = "";

    private static String COLD_STYLE_VAL = "";

    private static String CATEGORY_ID = "";

    //    private static final int PORT = 9001;
    private static int PORT = 8080;

    private static String ADD_PROD_REST = "";

    private static String UPDATE_PROD_REST = "";

    private static String GET_PROD_REST = "";

    private static final String DEL_PROD_REST = "/v1.0/products/{0}";


    private static void loadConfig(String configFile) throws IOException {
        Properties p = new Properties();
        p.load(new FileReader(new File(configFile)));
//        p.load(ColorMigrationWorker.class.getClassLoader().getResourceAsStream(configFile));
        STYLE_NODE_ID = p.getProperty("STYLE_NODE_ID");
        CONTENT_NODE_ID = p.getProperty("CONTENT_NODE_ID");
        PROD_NODE_ID = p.getProperty("PROD_NODE_ID");
        BRAND_SELECTED_ID = p.getProperty("BRAND_SELECTED_ID");
        RETAIL_SELECTED_ID = p.getProperty("RETAIL_SELECTED_ID");
        PAINT_PAINT = p.getProperty("PAINT_PAINT");
        CATALOG_HOST = p.getProperty("CATALOG_HOST");
        PORT = Integer.valueOf(p.getProperty("PORT"));
        CATEGORY_ID = p.getProperty("CATEGORY_ID");
        csvFileName = p.getProperty("csvFileName");
        successGUIDFile = p.getProperty("successGUIDFile");
        WARM_STYLE_VAL = p.getProperty("WARM_STYLE_VAL");
        COLD_STYLE_VAL = p.getProperty("COLD_STYLE_VAL");
        tenant = p.getProperty("tenant");
        successGUIDFile = successGUIDFile + "_" + CATEGORY_ID;
//        System.out.println("STYLE_NODE_ID " + STYLE_NODE_ID);
    }
    private static List<String[]> loadJsonFromCSV(String fileName) throws Exception {
        BufferedReader bf = new BufferedReader(new FileReader(new File(csvFileName)));
        String l;
        List<String[]> r = new ArrayList();
        int i = 0;
        l = bf.readLine(); //Skip the first line.
        while ((l = bf.readLine()) != null) {
//            if (i++ % 2 != 0) continue;
            String[] items = l.split(",");
            r.add(items);
        }
        return r;
    }

    private static String populateJson(String json, String rgb, String group, String style, String categoryId) {
        return MessageFormat.format(json, rgb, group, style, categoryId);
    }

    private static String populateJson2(String json, String... args) {
        return MessageFormat.format(json, args);
    }

    private static String arrayToString(String[] originItem) {
        StringBuilder buf = new StringBuilder();
        for (String s : originItem) {
            buf.append(s).append(",");
        }
        return buf.toString();
    }

    private static boolean outputResp(CloseableHttpResponse response, List<String> successGuids, String[] originItem) {
        StringBuilder respBuf = new StringBuilder();
        BufferedReader bf = null;
        try {
            HttpEntity resp = response.getEntity();
            if (resp != null) {
                String charset = EntityUtils.getContentCharSet(resp);
                bf = new BufferedReader(new InputStreamReader(resp.getContent(), charset));
                String l;
                while ((l = bf.readLine()) != null) {
                    System.out.println(l);
                    respBuf.append(l).append(" ---> ");
                    respBuf.append(arrayToString(originItem));
                    respBuf.append(System.getProperty("line.separator"));
                }
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    successGuids.add(respBuf.toString());
//TODO: Put result into a shared queue which will be accessed concurrently.
                    okQueue.put(respBuf.toString());
                    System.out.println("okQueue size --> " + okQueue.size());
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                bf.close();
                response.close();
            } catch (IOException e) {
                response = null;
                e.printStackTrace();
            }
        }
    }

    private static void consumeOkQueue() throws Exception {
        BufferedWriter fw = new BufferedWriter(new FileWriter(new File(successGUIDFile)));
        ExecutorService pe = Executors.newSingleThreadExecutor();
        System.out.println("Start to consume...");
        pe.submit(new Callable() {
            public String call() throws Exception {
                String okMsg = null;
                try {
                    while (true) {
                        try {
                            if (!isNew && isStop) {
                                break;
                            }
                            isNew = false;
                            System.out.println("Wait for take...");
                            okMsg = okQueue.take();
                            if(okMsg.startsWith("All done!"))
                                break;
                            System.out.println("Message " + okMsg + " consumed.");
                            fw.write(okMsg);
                            if (isStop && okQueue.isEmpty())
                                break;
                        } catch (InterruptedException e) {
                            System.out.println("okQueue.take() is interrupted.");
                            if (!okQueue.isEmpty()){
//                                fw.write(okMsg);
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                    pe.shutdown();
                    System.out.println("Consuming finished...");
                    return null;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    fw.close();
                }
                end = System.currentTimeMillis();
                System.out.println((end-start) / 1000 + " seconds.");
                return null;
            }
        });
    }

    private static CloseableHttpResponse restCall(CloseableHttpClient httpclient, RequestBuilder req,
                                                  boolean close, String httpMethod,String tenant) throws Exception {
        if (httpclient == null) return null;
        CloseableHttpResponse response = null;
        if ("POST".equalsIgnoreCase(httpMethod)) {
            HttpPost httpPost = new HttpPost(req.getRestUrl());
            StringEntity en = new StringEntity(req.getBody());
            en.setContentType("application/json");
            en.setContentEncoding("UTF-8");
            Date d = new Date();
            String ts = BackendHeaderUtil.genHsTs(tenant);
            String secret = BackendHeaderUtil.genHsSecurity(tenant);
            System.out.println("ts ---> " + ts);
            System.out.println("secret ---> " + secret);
            httpPost.addHeader("hs_ts",ts);
            httpPost.addHeader("hs_secret",secret);
            httpPost.setEntity(en);
            response = httpclient.execute(httpPost);
        } else if ("DELETE".equalsIgnoreCase(httpMethod)) {
            HttpDelete httpDel = new HttpDelete(req.getRestUrl());
            response = httpclient.execute(httpDel);
        }
        return response;
    }

    private static int submitJson(boolean concurrent, URI uri, CloseableHttpClient httpclient, String body, String categoryId,
                                  List<String> successGuids, AtomicInteger completedTasks, AtomicInteger successTasks, List<String[]> csvItems) throws Exception {
        ExecutorService es = null;
        if (concurrent) {
            es = Executors.newFixedThreadPool(5);
        }
        int successCnt = 0;
        HttpPost httpPost = new HttpPost(uri);
        for (int i = 1; i < csvItems.size(); i++) {
            System.out.println(i);
            String[] items = csvItems.get(i);
            if (items == null || items.length == 0) break;
            String style = "";
            if (items[ARGS.GROUP.value()].equals("orange") ||
                    items[ARGS.GROUP.value()].equals("gold") ||
                    items[ARGS.GROUP.value()].equals("red") ||
                    items[ARGS.GROUP.value()].equals("yellow")
                    ) {
//                style = "f7c985ee-ca1b-499f-aace-e1b8923cb177"; //Warm style
                style = WARM_STYLE_VAL;
            } else {
//                style = "2079c05a-1b04-4314-9cbb-320da25acce0"; //Cold style
                style = COLD_STYLE_VAL;
            }

            String jsonBody = populateJson(body, items[ARGS.RGB.value()], items[ARGS.GROUP.value()], style, categoryId);
            System.out.println(jsonBody);
            StringEntity en = new StringEntity(jsonBody);
            en.setContentType("application/json");
            en.setContentEncoding("UTF-8");
            httpPost.setEntity(en);
            if (es != null) {
                Future<String> f = es.submit(new Callable<String>() {
                    public String call() throws Exception {
                        boolean success = false;
                        CloseableHttpResponse response = httpclient.execute(httpPost);
                        if (outputResp(response, successGuids, items)) {
                            success = true;
                            System.out.println("Successfully added 1 record.");
                        } else {
                            success = false;
                            System.err.println("Failed to add 1 record");
                        }
                        if (completedTasks.incrementAndGet() == csvItems.size() - 1) {
                            okQueue.put("All done!");
                            isStop = true;
                            Thread.currentThread().interrupt();
                        }
                        if (success) {
                            successTasks.incrementAndGet();
                            return "";
                        } else
                            return null;
                    }
                });
            } else {
                CloseableHttpResponse response = httpclient.execute(httpPost);
                outputResp(response, successGuids, items);
                if (completedTasks.incrementAndGet() == csvItems.size() - 1) {
                    isStop = true;
                    Thread.currentThread().interrupt();
                }
            }
            System.out.println("Processed 1 csv item...");
        }
        if (es != null) {
            es.shutdown();
        }
        return successTasks.get();
    }

    public void removeProd(String[] prodIds) {
        String _delRest = DEL_PROD_REST;

        String delRest = populateJson2(_delRest, prodIds);
    }

    private static abstract class RequestBuilder {
        private URI restUrl;
        private String body;

        public URI getRestUrl() {
            return restUrl;
        }

        public void setRestUrl(URI restUrl) {
            this.restUrl = restUrl;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }

    private static class AddProductReqBuilder extends RequestBuilder {
        public AddProductReqBuilder(String[] cvsItem, String categoryId, String tenantId) throws Exception {
            String style = "";
            if (cvsItem[ARGS.GROUP.value()].equals("orange") ||
                    cvsItem[ARGS.GROUP.value()].equals("gold") ||
                    cvsItem[ARGS.GROUP.value()].equals("red") ||
                    cvsItem[ARGS.GROUP.value()].equals("yellow")
                    ) {
//                style = "f7c985ee-ca1b-499f-aace-e1b8923cb177"; //Warm style
                style = WARM_STYLE_VAL; //Warm style
            } else {
//                style = "2079c05a-1b04-4314-9cbb-320da25acce0"; //Cold style
                style = COLD_STYLE_VAL; //Cold style
            }
            String rgbStr = cvsItem[ARGS.RGB.value()];
            try {
                Integer.parseInt(rgbStr);
            } catch(NumberFormatException e) {
                throw new IllegalArgumentException("Invalid RGB value: " + rgbStr);
            }
            String jsonBody = populateJson2(ADD_PROD_REST, new String[]{rgbStr, cvsItem[ARGS.GROUP.value()],
                    style, categoryId});
            URI uri = new URIBuilder()
                    .setScheme("http")
                    .setHost(CATALOG_HOST)
                    .setPort(PORT)
                    .setPath("/v1.0/products")
                    .setParameter("t", tenantId)
                    .setParameter("generateTopView", "false")
                    .setParameter("lang", "en_US")
                    .build();
            setRestUrl(uri);
            setBody(jsonBody);
            System.out.println("Rest url: " + uri.toString());
            System.out.println(jsonBody);
        }
    }

    private static class DelProductReqBuilder extends RequestBuilder {
        public DelProductReqBuilder(String[] cvsItem, String categoryId, String tenantId) throws Exception  {
            String jsonBody = populateJson2(DEL_PROD_REST, new String[]{tenantId});
            URI uri = new URIBuilder()
                    .setScheme("http")
                    .setHost(CATALOG_HOST)
                    .setPort(PORT)
                    .setPath(populateJson2(DEL_PROD_REST, new String[]{cvsItem[0]}))
                    .setParameter("t", tenantId)
                    .setParameter("generateTopView", "false")
                    .setParameter("lang", "en_US")
                    .build();
            setRestUrl(uri);
            setBody("");
            System.out.println(uri.toString());
        }
    }

    public static void removeProdFromFile(String f) throws Exception {
        List<String[]> csvItems = loadJsonFromCSV(f);
    }

    public static void addProdFromFile(String f, String categoryId, String tenantId, boolean concurrent) throws Exception {
        List<String[]> csvItems = loadJsonFromCSV(f);
        CloseableHttpClient c = HttpClients.createDefault();
        AtomicInteger completedTasks = new AtomicInteger(0);

        ExecutorService es = null;
        if (concurrent) {
            es = Executors.newFixedThreadPool(10);
        }
        int successCnt = 0;

        for(String[] csvItem : csvItems) {
            if (es != null) {
                Future<String> f1 = es.submit(new Callable<String>() {
                    public String call() throws Exception {
                        boolean success = false;
                        AddProductReqBuilder addProdReq = null;
                        try {
                            addProdReq = new AddProductReqBuilder(csvItem, categoryId, tenantId);
                        } catch (Exception e) {
                            completedTasks.incrementAndGet();
                            System.err.println("Failed to process csv item: " + csvItem[0] + ", " + csvItem[1]);
                        }
                        CloseableHttpResponse response = restCall(c, addProdReq, false, "post", tenantId);
                        outputResp(response, new ArrayList<String>(), csvItem);

                        if (completedTasks.incrementAndGet() == csvItems.size()) {
                            okQueue.put("All done!");
                            isStop = true;
                            Thread.currentThread().interrupt();
                        }
                        return String.valueOf(completedTasks.get());
                    }
                });
            } else {
                AddProductReqBuilder addProdReq = null;
                try {
                    addProdReq = new AddProductReqBuilder(csvItem, categoryId, tenantId);
                } catch (Exception e) {
                    System.err.println("Failed to process csv item: " + csvItem[0] + ", " + csvItem[1]);
                    completedTasks.incrementAndGet();
                    continue;
                }
                CloseableHttpResponse response = restCall(c, addProdReq, false, "post", tenantId);
                outputResp(response, new ArrayList<String>(), csvItem);
                System.out.println("Completed tasks count: " + completedTasks.get() + " csvItem size: " + csvItems.size());
                if (completedTasks.incrementAndGet() == csvItems.size()) {
                    System.out.println("Put all done!");
                    okQueue.put("All done!");
                    isStop = true;
//                Thread.currentThread().interrupt();//No use here!
                }
            }
        }
        if(es != null) {
            while (true) {
                if (isStop) {
                    System.out.println("Shut down producer service");
                    es.shutdown();
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 0) {
            loadConfig(args[0]);
        } else {
            throw new IllegalArgumentException("Please specify the color upload configuration file.");
        }
//        loadConfig("colorHsm_upload_prod.properties");
        ADD_PROD_REST = "'{'\"name\" : \"{0}\",\"defaultName\" : \"{0}\",\"description\" : \" \",\"defaultDescription\" : \" \",\"status\" : 1,\"newFlag\" : true,\"attributes\" : [" +
                "'{'\"id\" : \"" + STYLE_NODE_ID + "\",\"valuesIds\" : [\"{2}\"]'}', " + //style
                "'{'\"id\" : \"" + CONTENT_NODE_ID + "\",\"valuesIds\" : [\"" + PAINT_PAINT + "\"]'}'," + //content type
                "'{'\"id\" : \"" + PROD_NODE_ID + "\",\"valuesIds\" : [],\"free\" : [\"{0}\",\"{1}\",\"{2}\"]'}']" + //product type
                ",\"brands\" : [\"" + BRAND_SELECTED_ID + "\"]" +
                ",\"retailers\" : ['{'\"id\" : \"" + RETAIL_SELECTED_ID + "\"'}']" +
                ",\"categories\" : [\"{3}\"],\"references\" : '{}',\"files\" : []'}'";
//        }

        UPDATE_PROD_REST = "'{'\"defaultName\" : \"{0}\",\"description\" : \" \",\"defaultDescription\" : \" \",\"status\" : 1,\"newFlag\" : true,\"attributes\" : [" +
                "'{'\"id\" : \"" + STYLE_NODE_ID + "\",\"valuesIds\" : [\"{2}\"]'}', " + //style
                "'{'\"id\" : \"" + CONTENT_NODE_ID + "\",\"valuesIds\" : [\"" + PAINT_PAINT + "\"]'}'," + //content type
                "'{'\"id\" : \"" + PROD_NODE_ID + "\",\"valuesIds\" : [],\"free\" : [\"{0}\",\"{1}\",\"{2}\"]'}']" + //product type
                ",\"brands\" : [\"" + BRAND_SELECTED_ID + "\"]" +
                ",\"retailers\" : ['{'\"id\" : \"" + RETAIL_SELECTED_ID + "\"'}']" +
                ",\"categories\" : [\"{3}\"],\"references\" : '{}',\"files\" : []'}'";

        GET_PROD_REST = "";

/*        String categoryId = null;
        String tenant = null;
        if (args.length > 0) {
            categoryId = args[0];
            tenant = args[1];
        } else {
//            categoryId = "d72316f9-ed94-4872-9663-206471783a18";
            categoryId = CATEGORY_ID;
            tenant = "ezhome";
        }*/
//        List<String> successGuids = new ArrayList();
        String categoryId = CATEGORY_ID;
        start = System.currentTimeMillis();
        consumeOkQueue();
        addProdFromFile(csvFileName, categoryId, tenant, false);
        end = System.currentTimeMillis();
        System.out.println("Takes " + (end - start) / 1000 + " seconds.");
    }
}
