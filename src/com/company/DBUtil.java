package com.company;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by yangg on 2016/1/14.
 */
public class DBUtil {

    private static String dbUrl;

    private static DBUtil inst;

    private String userName;

    private String password;

    private Connection conn;

    private static String JSON_FILE_URL = "c:\\color_test_data\\json_urls.txt";

    public static synchronized DBUtil getInstance(String propFile) throws IOException {
        if(inst == null) {
            inst = new DBUtil(propFile);
        }
        return inst;
    }

    private DBUtil(String propFile) throws IOException {
        Properties p = new Properties();
        p.load(new FileReader(new File(propFile)));
        if((dbUrl = p.getProperty("DB_URL")) == null) throw new InvalidPropertiesFormatException("Missing key DB_URL");
        if((userName = p.getProperty("usrName")) == null) throw new InvalidPropertiesFormatException("Missing key usrName");
        if((password = p.getProperty("pwd")) == null) throw new InvalidPropertiesFormatException("Missing key pwd");
    }

    private void initMySqlConn() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch(ClassNotFoundException e){
            e.printStackTrace();
            return;
        }
        try {
            System.out.println("DB_URL ---> " + dbUrl);
            conn = DriverManager.getConnection(dbUrl, userName, password);
        }catch(SQLException se) {
            se.printStackTrace();
        }
//        conn.prepareStatement()
    }

    public List<String> fetchS3JsonListFromDB(String tenant, String hostName) {
        System.out.println("Starting in fetchS3JsonListFromDB...");
        List<String> jsonDataList = new ArrayList();
        if(conn == null) {
            initMySqlConn();
        }
        String sql = "SELECT a.tenant, a.Data FROM hscontent.Assets a where tenant=?";
        PreparedStatement pStat = null;
//        File f = new File(jsonUrlFile);
        BufferedWriter bof = null;
        try {
//            bof = new BufferedWriter(new FileWriter(f));
//            try {
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
//                    bof.write(d);
//                    bof.write("\r\n");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                releaseMysqlConn(pStat, conn);
            }
/*       } catch (IOException e) {
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
        }*/
        return jsonDataList.stream().filter(l -> (l.indexOf(hostName) > 0)).collect(Collectors.toList());
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
}
