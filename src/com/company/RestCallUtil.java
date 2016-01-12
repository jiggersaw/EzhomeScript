package com.company;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangg on 2016/1/7.
 */
public class RestCallUtil {

    public static String logFile = "C:\\color_test_data\\branch_fix.log";

    // For idealstandard global alpha
    private static String STYLE_NODE_ID = "9221463d-18ca-4b63-b09b-a2000585a00b";
    private static String CONTENT_NODE_ID = "6d6d224e-bae0-4045-8c98-b044332ee7aa";
    private static String PROD_NODE_ID = "f2ebc921-291a-42c6-be8d-e6f3019672fb";
    private static String BRAND_SELECTED_ID = "44fb6476-b446-425b-a1f7-c933142746b9";
    private static String PAINT_PAINT = "1a8d33f3-2f6d-400f-8a0b-10c65c95361f";
    private static String RETAIL_SELECTED_ID = "de9d4c65-f747-4963-acfa-cea44c475fcc";
    private static String CATALOG_HOST = "http://catalog-be.alpha.homestyler.com/";
    private static String CATALOG_ID = "bf6eaf76-6dd9-43c7-bba9-fc03f2ab5fdb";
    private static String WARM_STYLE_VAL = "";
    private static String COLD_STYLE_VAL = "";
    private static String BRANCH = "GB";

    public static String simpleRestCall(String url, String body, String tenant, String httpMethod) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        if (httpclient == null) return null;
        CloseableHttpResponse response = null;
        if ("POST".equalsIgnoreCase(httpMethod)) {
            HttpPost httpPost = new HttpPost(url);
            StringEntity en = new StringEntity(body);
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
            HttpDelete httpDel = new HttpDelete(url);
            response = httpclient.execute(httpDel);
        } else if("GET".equalsIgnoreCase(httpMethod)) {
            HttpGet httpGet = new HttpGet(url);
            String ts = BackendHeaderUtil.genHsTs(tenant);
            String secret = BackendHeaderUtil.genHsSecurity(tenant);
            System.out.println("ts ---> " + ts);
            System.out.println("secret ---> " + secret);
            httpGet.setHeader("hs_ts",ts);
            httpGet.setHeader("hs_secret",secret);
            response = httpclient.execute(httpGet);
        } else if("PUT".equalsIgnoreCase(httpMethod)) {
            HttpPost httpPost = new HttpPost(url);
            StringEntity en = new StringEntity(body);
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
        }
        String result = streamToString(response.getEntity().getContent(), EntityUtils.getContentCharSet(response.getEntity()));
        return result;
    }

    public static String[] batchRestCall(String[] urls, String[] body, String tenant, String httpMethod) throws IOException {
        File log = new File(logFile);

        String[] resps = new String[urls.length];
        if("POST".equalsIgnoreCase(httpMethod)) {
            if(urls.length != body.length) {
                throw new IllegalArgumentException("Rest urls length should be same as body length.");
            }
        }
        AtomicInteger k = new AtomicInteger(0);
        Arrays.asList(urls).stream().parallel().forEach(u -> {
            try {
                int x = k.incrementAndGet();
                System.out.println("Finish " + x + " call ----> " + urls[x-1]);
                FileUtil.appendToFile(log, "Finish " + x + " call ----> " + urls[x-1]);
                resps[x-1] = simpleRestCall(u, null, tenant, httpMethod);
                FileUtil.appendToFile(log, "Response ---> " + resps[x-1] + " for " + urls[x-1]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        FileUtil.finishAppend(log);
//        for(int i=0; i<urls.length; i++) {
//            if(body == null) {
//                resps[i] = simpleRestCall(urls[i], null, tenant, httpMethod);
//            } else {
//                resps[i] = simpleRestCall(urls[i], body[i], tenant, httpMethod);
//            }
//            System.out.println("Finish 1 call ----> " + urls[i]);
//        }
        return resps;
    }

    private static String streamToString(InputStream in, String charset) throws IOException {
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new InputStreamReader(in, charset));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        StringBuilder respBuf = new StringBuilder();
        String l;
        while ((l = bf.readLine()) != null) {
            System.out.println(l);
            respBuf.append(l);
            respBuf.append(System.getProperty("line.separator"));
        }
        return respBuf.toString();
    }

    private static String arrayToString(String[] originItem) {
        StringBuilder buf = new StringBuilder();
        for (String s : originItem) {
            buf.append(s).append(",");
        }
        return buf.toString();
    }

    public static void main(String[] args) throws IOException {
        String jsonTemplate = "http://3d.juran.cn/api/rest/v2.0/product/{0}?t={1}&l=en_US";

        List<String> ids = FileUtil.readFileAsLines(new File("C:\\color_test_data\\wrong_floor.txt"));
        String[] urls = new String[ids.size()];
        for(int i=0; i<urls.length; i++) {
            urls[i] = MessageFormat.format(jsonTemplate, new String[]{ids.get(i), "ezhome"});
        }
        String[] resps = batchRestCall(urls, null, "ezhome", "GET");
        for(String r : resps) {
            Arrays.asList(JsonWorker.getDimFromContent(r)).forEach(d -> System.out.print(d + ", "));
            System.out.println();
        }

        String updateProdUrl = CATALOG_HOST + "/v1.0/products/{0}?lang=en_US&t={1}&tm=false&branch={2}";
        String updateProd = "'{'\"defaultName\" : \"{0}\",\"description\" : \" \",\"defaultDescription\" : \" \",\"status\" : 1,\"newFlag\" : true,\"attributes\" : [" +
                "'{'\"id\" : \"" + STYLE_NODE_ID + "\",\"valuesIds\" : [\"{2}\"]'}', " + //style
                "'{'\"id\" : \"" + CONTENT_NODE_ID + "\",\"valuesIds\" : [\"" + PAINT_PAINT + "\"]'}'," + //content type
                "'{'\"id\" : \"" + PROD_NODE_ID + "\",\"valuesIds\" : [],\"free\" : [\"{0}\",\"{1}\",\"{2}\"]'}']" + //product type
                ",\"brands\" : [\"" + BRAND_SELECTED_ID + "\"]" +
                ",\"retailers\" : ['{'\"id\" : \"" + RETAIL_SELECTED_ID + "\"'}']" +
                ",\"categories\" : [\"{3}\"],\"references\" : '{}',\"files\" : []'}'";
        List<String> params = FileUtil.readFileAsLines(new File("C:\\color_test_data\\idealstandard_to_fix.txt"));
        String[] urls2 = new String[params.size()];
        String[] jsonBody = new String[params.size()];
        String[] newParams = new String[4];
        for(int j=0;j<urls2.length;j++) {
            String l = params.get(j);
            String[] arr = l.split(",");
            newParams[0] = arr[1].trim();//rgb
            newParams[1] = arr[2].trim();
            String style;
            if (newParams[1].equals("orange") ||
                    newParams[1].equals("gold") ||
                    newParams[1].equals("red") ||
                    newParams[1].equals("yellow")
                    ) {
//                style = "f7c985ee-ca1b-499f-aace-e1b8923cb177"; //Warm style
                style = WARM_STYLE_VAL;
            } else {
                style = COLD_STYLE_VAL;
            }
            newParams[2] = style;//style
            newParams[3] = CATALOG_ID;
            urls2[j] = MessageFormat.format(updateProd, newParams);
            jsonBody[j] = MessageFormat.format(updateProdUrl, new String[]{arr[0], BRANCH});
        }
        System.out.println("Batch call done!");
    }
}
