package com.company;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangg on 2015/12/23.
 */
public class FileUtil {

    private static BufferedWriter bf = null;

    private static Map<String, BufferedWriter> bufWriterCache = new HashMap();

    public static synchronized void appendToFile(File f, String s) {
        try {
            bf = bufWriterCache.get(f.getCanonicalPath());
            if(bf == null) {
                bf = new BufferedWriter(new FileWriter(f));
                bufWriterCache.put(f.getCanonicalPath(), bf);
            }
            bf.write(s);
            bf.write(System.getProperty("line.separator"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void finishAppend(File f) throws IOException {
        BufferedWriter w = bufWriterCache.get(f.getCanonicalPath());
        if(w != null) {
            w.close();
            bufWriterCache.remove(f.getCanonicalPath());
            w = null;
        }
    }

    public static void writeToFile(File f, String s) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write(s);
        bw.close();
        bw = null;
    }

    public static String readFileAsString(File f) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(f));
        StringBuilder buf = new StringBuilder();
        String l = "";
        try {
            while ((l = br.readLine()) != null) {
                if (l.trim().length() == 0) continue;
                buf.append(l);
                buf.append(System.getProperty("line.separator"));
            }
        } finally {
            br.close();
        }
        return buf.toString();
    }

    public static List<String> readFileAsLines(File f) throws IOException {
        List<String> buf = new ArrayList();
        BufferedReader br = null;
        String l = "";
        try {
            br = new BufferedReader(new FileReader(f));
            while((l = br.readLine()) != null) {
                if(l.trim().length() == 0) continue;
                buf.add(l);
            }
        } finally {
           if(br != null){
               br.close();
           }
        }
        return buf;
    }
}
