package com.company;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

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

    private static String csvFileName = "F:\\color_test_data\\color_final.csv";

    private static String successGUIDFile = "F:\\color_test_data\\guids.txt";

    private static ArrayBlockingQueue<String> okQueue = new ArrayBlockingQueue(9);

    private static volatile boolean isStop = false;

    private static List<String[]> loadJsonFromCSV(String fileName) throws Exception {
        BufferedReader bf = new BufferedReader(new FileReader(new File(csvFileName)));
        String l;
        List<String[]> r = new ArrayList();
        while ((l = bf.readLine()) != null) {
            String[] items = l.split(",");
            r.add(items);
        }
        return r;
    }

    private static String populateJson(String json, String rgb, String group, String style, String categoryId) {
        return MessageFormat.format(json, rgb, group, style, categoryId);
    }

//
//    public static void foo() {
//        String body = "'{'\"name\" : \"{0}\",\"defaultName\" : \"{0}\",\"description\" : \" \",\"defaultDescription\" : \" \",\"status\" : 1,\"newFlag\" : true,\"attributes\" : ['{'\"id\" : \"9221463d-18ca-4b63-b09b-a2000585a00b\",\"valuesIds\" : [\"7511b29a-1d8b-4166-b8af-a09e57b8ba7b\"]'}','{'\"id\" : \"f2ebc921-291a-42c6-be8d-e6f3019672fb\",\"valuesIds\" : [\"001e1123-52fb-42d4-a4fe-beff24011986\"],\"free\" : [\"{0}\",\"{1}\",\"{2}\"]'}'],\"brands\" : [\"44fb6476-b446-425b-a1f7-c933142746b9\"],\"retailers\" : ['{'\"id\" : \"de9d4c65-f747-4963-acfa-cea44c475fcc\"'}'],\"categories\" : [\"{3}\"],\"references\" : '{}',\"files\" : []'}'";
//        System.out.println(MessageFormat.format(body, "cc16", "group1", "warm", "10c9b6e4-0d63-44ff-be65-f56354a50a4a"));
//    }

    private static boolean showResp(CloseableHttpResponse response, List<String> successGuids) {
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
                    respBuf.append(l);
                }
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    successGuids.add(respBuf.toString());
                    //TODO: Put result into a shared queue which will be accessed concurrently.
                    okQueue.put(respBuf.toString());
                    System.out.println("okQueue size --> " + okQueue.size());
                    return true;
//                    Reader r = new InputStreamReader(resp.getContent(), charset);

//                    InputStream in = resp.getContent();
//                    byte[] b = new byte[4096];
//                    StringBuilder buf = new StringBuilder();
//                    for (int n; (n = in.read(b)) != -1; ) {
//                        buf.append(n);
//                    }
//                    in.close();
//                    System.out.println(buf.toString());
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
                    while (!isStop) {
                        okMsg = okQueue.take();
                        System.out.println("After consuming, okQueue size --> " + okQueue.size());
                        fw.write(okMsg);
                    }
                    return null;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    fw.close();
                }
                return null;
            }
        });
    }

    public static void main(String[] args) throws Exception {
        // write your code here
//        foo();
        String body = "'{'\"name\" : \"{0}\",\"defaultName\" : \"{0}\",\"description\" : \" \",\"defaultDescription\" : \" \",\"status\" : 1,\"newFlag\" : true,\"attributes\" : ['{'\"id\" : \"9221463d-18ca-4b63-b09b-a2000585a00b\",\"valuesIds\" : [\"7511b29a-1d8b-4166-b8af-a09e57b8ba7b\"]'}','{'\"id\" : \"f2ebc921-291a-42c6-be8d-e6f3019672fb\",\"valuesIds\" : [\"001e1123-52fb-42d4-a4fe-beff24011986\"],\"free\" : [\"{0}\",\"{1}\",\"{2}\"]'}'],\"brands\" : [\"44fb6476-b446-425b-a1f7-c933142746b9\"],\"retailers\" : ['{'\"id\" : \"de9d4c65-f747-4963-acfa-cea44c475fcc\"'}'],\"categories\" : [\"{3}\"],\"references\" : '{}',\"files\" : []'}'";
        String categoryId = null;
        String tenant = null;
        if (args.length > 0) {
            categoryId = args[0];
            tenant = args[1];
        } else {
            categoryId = "10c9b6e4-0d63-44ff-be65-f56354a50a4a";
            tenant = "testqa";
        }

        List<String> successGuids = new ArrayList();
        List<String[]> csvItems = loadJsonFromCSV(csvFileName);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        URI uri;
        uri = new URIBuilder()
                .setScheme("http")
                .setHost("localhost")
                .setPort(9001)
                .setPath("/v1.0/products")
                .setParameter("t", tenant)
                .setParameter("generateTopView", "true")
                .setParameter("lang", "en_US")
                .build();
        ExecutorService es = Executors.newFixedThreadPool(10);
        consumeOkQueue();
        AtomicInteger compleltedTasks = new AtomicInteger(0);
        for (int i = 1; i < csvItems.size(); i++) {
            String[] items = csvItems.get(i);
            if (items == null || items.length == 0) break;
            String style = "";
            if (items[ARGS.GROUP.value()].equals("orange") ||
                    items[ARGS.GROUP.value()].equals("gold") ||
                    items[ARGS.GROUP.value()].equals("red") ||
                    items[ARGS.GROUP.value()].equals("yellow")
                    ) {
                style = "warm";
            } else {
                style = "cold";
            }
            String jsonBody = populateJson(body, items[ARGS.RGB.value()], items[ARGS.GROUP.value()], style, "10c9b6e4-0d63-44ff-be65-f56354a50a4a");
            System.out.println(jsonBody);

            es.submit(new Callable<String>() {
                public String call() throws Exception {
                    HttpPost httpPost = new HttpPost(uri);
                    StringEntity en = new StringEntity(jsonBody);
                    en.setContentType("application/json");
                    en.setContentEncoding("UTF-8");
                    httpPost.setEntity(en);
                    CloseableHttpResponse response = httpclient.execute(httpPost);
                    if (showResp(response, successGuids)) {
                        System.out.println("Successfully added 1 record.");
                    } else {
                        System.err.println("Failed to add 1 record");
                    }
                    int completedCnt = compleltedTasks.incrementAndGet();
                    if (completedCnt == csvItems.size() - 1)
                        isStop = true;
                    return null;
                }
            });

        }
    }
}
