package com.company;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.Date;

/**
 * Created by yangg on 2016/1/5.
 */
public class BackendHeaderUtil {

    private static String ts;

    public static String genHsTs(String tenant) {
        ts = String.valueOf(new Date().getTime());
        return ts;
    }

    public static String genHsSecurity(String tenant) {
        if(ts == null){
            ts = genHsTs(tenant);
        }
        String secret = DigestUtils.sha256Hex(ts + "adsk-" + tenant);
        return secret;
    }

    public static void main(String[] args) {
        System.out.println("hs_ts: " + genHsTs("ezhome"));
        System.out.println("hs_secret: " + genHsSecurity("ezhome"));
    }
}
