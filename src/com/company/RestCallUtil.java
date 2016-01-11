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
        }
        String result = streamToString(response.getEntity().getContent(), EntityUtils.getContentCharSet(response.getEntity()));
        return result;
    }

    public static String[] batchRestCall(String[] urls, String[] body, String tenant, String httpMethod) throws IOException {
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
                resps[x-1] = simpleRestCall(u, null, tenant, httpMethod);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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
        System.out.println("Batch call done!");
    }
}
