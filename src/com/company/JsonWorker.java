package com.company;

import com.amazonaws.util.Md5Utils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by yangg on 2015/12/23.
 */
public class JsonWorker {

    private static String testJson = "";

    private static ObjectMapper mapper = new ObjectMapper();

    private static List<ObjectMapper> mapperPool = new ArrayList();

    private static AtomicInteger poolPos = new AtomicInteger(0);

    private static int POOL_SIZE = 10;

    static {
        for(int i=0; i<POOL_SIZE; i++) {
            mapperPool.add(new ObjectMapper());
        }
    }

/*
    public void filter_map_by_entries_java8_lambda () {
        HashMap<String, String> MONTHS = new HashMap();
        Map<Integer, String> monthsWithLengthFour =
                MONTHS.entrySet()
                        .stream()
                        .filter(p -> p.getValue().length() == 4)
                        .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));

//        assertThat(monthsWithLengthFour.values(), contains("June", "July"));
    }
*/

    public static boolean updateWallPaint2(String jsonData, Map<String, String[]> seekIdMap, String categoryId, StringBuilder holder) throws Exception {
        ObjectMapper mapper = mapperPool.get(poolPos.getAndIncrement() % POOL_SIZE);
        String rs = null;
        try {
            Map m = mapper.readValue(jsonData, Map.class);
            List<Map> l = (List) m.get("data");
            final AtomicBoolean updated = new AtomicBoolean(false);
            l.forEach(m1 -> {
                if ("hsw.model.Material".equals(m1.get("Class"))) {
                    if (seekIdMap.get(m1.get("seekId")) != null) {
                        String[] colorData = seekIdMap.get(m1.get("seekId"));
                        System.out.println("FOUND OLD DESIGN TO BE UPDATED...");
                        m1.put("seekId", colorData[0]);
                        m1.put("textureURI", "DDD");
                        m1.remove("name");
                        m1.put("categoryId", categoryId);
                        Integer color = null;
                        try {
                            color = Integer.valueOf(colorData[1]);
                            m1.put("color", color);
                            updated.set(true);
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            if(updated.get()) {
                rs = mapper.writeValueAsString(m);
                holder.append(rs);
            }
            return updated.get();
        } catch(Exception e) {
            throw e;
        }
    }

    public static Float[] parseXYZFromJson(String jsonData) {
        ObjectMapper mapper = mapperPool.get(poolPos.getAndIncrement() % POOL_SIZE);
        Float[] xyz = new Float[3];
        try {
            Map m = mapper.readValue(jsonData, Map.class);
            List<Map> files = (List)m.get("files");
            List<Map> topViews = files.stream().filter(m2 -> m2.get("metaData").equals("topView")).collect(Collectors.toList());
            Map m1 = topViews.get(0);
            String url = (String)m1.get("url");
            String tmp = url.substring(url.indexOf('?')+1, url.length());
            String[] dims = tmp.split("&");
            if(dims.length > 0) {
                Float fx = Float.valueOf(dims[0].split("=")[1]);
                Float fy = Float.valueOf(dims[1].split("=")[1]);
                Float fz = Float.valueOf(dims[2].split("=")[1]);
                xyz[0] = fx;
                xyz[1] = fy;
                xyz[2] = fz;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return xyz;
    }

    public static boolean updateContentDimension(String jsonData, Map<String, Float[]> contentDimMap, StringBuilder holder) throws JsonParseException, JsonMappingException {
        ObjectMapper mapper = mapperPool.get(poolPos.getAndIncrement() % POOL_SIZE);
        String rs;
        try {
            Map m = mapper.readValue(jsonData, Map.class);
            List<Map> l = (List) m.get("data");
            final AtomicBoolean updated = new AtomicBoolean(false);
            final AtomicInteger updateCnt = new AtomicInteger(0);
            if(l == null) {
                System.out.println("Got null...");
            }
            l.forEach(m1 -> {
                if("hsw.model.Content".equals(m1.get("Class")) && m1.get("seekId") != null &&
                        ((String)m1.get("seekId")).trim().length() > 0 &&
                        contentDimMap.containsKey(((String)m1.get("seekId")).trim())) {
                    String unit = (String)m1.get("unit");
                    Float x = contentDimMap.get(m1.get("seekId"))[0];
                    Float y = contentDimMap.get(m1.get("seekId"))[1];
                    Float z = contentDimMap.get(m1.get("seekId"))[2];
                    System.out.println("XLength before fix: " + m1.get("XLength") + "|XLength after fix: " + x);
                    System.out.println("YLength before fix: " + m1.get("YLength") + "|YLength after fix: " + y);
                    System.out.println("ZLength before fix: " + m1.get("ZLength") + "|ZLength after fix: " + z);
                    m1.put("XLength", String.valueOf(x));
                    m1.put("YLength", String.valueOf(y));
                    m1.put("ZLength", String.valueOf(z));
                    updated.set(true);
                    updateCnt.incrementAndGet();
                }
            });
            if(updated.get()) {
                rs = mapper.writeValueAsString(m);
                holder.append(rs);
                System.out.println("----------Found " + updateCnt + " content dimension fixed in 1 design----------");
                return updated.get();
            }
        } catch(JsonParseException e1) {
            throw e1;
        } catch(JsonMappingException e2) {
            throw e2;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static Object[] getDimFromContent(String jsonData) {
        ObjectMapper mapper = mapperPool.get(poolPos.getAndIncrement() % POOL_SIZE);
        try {
            Map m = mapper.readValue(jsonData, Map.class);
            Map l = (Map) m.get("item");
            String id = (String) l.get("id");
            Map dim = (Map) l.get("boundingBox");
            if(dim != null) {
                Float x = ((Double) dim.get("xLen")).floatValue();
                Float y = ((Double)dim.get("yLen")).floatValue();
                Float z = ((Double)dim.get("zLen")).floatValue();
                return new Object[] {id, x, y, z};
            } else {
                System.err.println("Invalid content json...");
                return null;
            }
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String tryReadCorrectJson(String bucket, String key, String md5OnS3, File failedLog) {
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

    public static boolean updateWallPaint(String jsonData, Map<String, String[]> seekIdMap, String categoryId, StringBuilder holder) throws Exception {
        ObjectMapper mapper = mapperPool.get(poolPos.getAndIncrement() % POOL_SIZE);
        String rs = null;
        try {
            Map m = mapper.readValue(jsonData, Map.class);
            List<Map> l = (List) m.get("data");
            final AtomicBoolean updated = new AtomicBoolean(false);
            final AtomicInteger updateCnt = new AtomicInteger(0);
            l.forEach(m1 -> {
                if ("hsw.model.Material".equals(m1.get("Class")) && m1.get("seekId") != null &&
                        ((String)m1.get("seekId")).trim().length() > 0) {
                    if (seekIdMap.get(m1.get("seekId")) != null) {
                        String[] colorData = seekIdMap.get(m1.get("seekId"));
//                        System.out.println("FOUND OLD DESIGN TO BE UPDATED...");
                        m1.put("seekId", colorData[0]);
                        m1.put("textureURI", "");
                        m1.remove("name");
                        m1.put("categoryId", categoryId);
                        Integer color = null;
                        try {
                            color = Integer.valueOf(colorData[1]);
                            m1.put("color", color);
                            updated.set(true);
                            updateCnt.incrementAndGet();
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            if(updated.get()) {
                rs = mapper.writeValueAsString(m);
                holder.append(rs);
                System.out.println("----------Found " + updateCnt + " wall paint updated in 1 design----------");
            }
            return updated.get();
        } catch(Exception e) {
            throw e;
        }
    }

    public static String extractKeyFromUrl(String jsonUrl) {
        int idx = jsonUrl.indexOf("Asset");
        String keyName = jsonUrl.substring(idx);
        String jsonFileName = jsonUrl.substring(jsonUrl.lastIndexOf("/") + 1);
        if(idx <= 0 ){
            return null;
        }
        return keyName;
    }

    public static void main(String[] args) throws IOException {
        String s = FileUtil.readFileAsString(new File("C:\\color_test_data\\json_tmp\\7d99c5a1-3759-4706-88e8-53a68a2b297e.json"));
        System.out.println(s);
        String colorCategoryId = "d72316f9-ed94-4872-9663-206471783a18";
//        FileUtil.writeToFile(new File("c:\\json_sample_out.json"), s);
        StringBuilder ss = new StringBuilder();

        try {
            updateWallPaint(s, ColorMigrationWorker.loadColorMap(), colorCategoryId, ss);
            FileUtil.writeToFile(new File("h:\\big_json.json"), ss.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
