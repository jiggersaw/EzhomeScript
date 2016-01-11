package com.company;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

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
        String result = streamToString(response.getEntity().getContent());
        return result;
    }

    public static String[] batchRestCall(String[] urls, String[] body, String tenant, String httpMethod) throws IOException {
        String[] resps = new String[urls.length];
        if("POST".equalsIgnoreCase(httpMethod)) {
            if(urls.length != body.length) {
                throw new IllegalArgumentException("Rest urls length should be same as body length.");
            }
            for(int i=0; i<urls.length; i++) {
                resps[i] = simpleRestCall(urls[i], body[i], tenant, httpMethod);
            }
        } else {

        }
        return resps;
    }

    private static String streamToString(InputStream in) {
        return null;
    }
}
