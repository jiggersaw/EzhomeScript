package com.company;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.Date;

/**
 * Created by yangg on 2015/12/21.
 */
public class TenantHeaderSecurity {

    public static void main(String[] args) {
        Date d = new Date();
        String tenant = "ezhome";
        String ts = String.valueOf(new Date().getTime());
        String secret = DigestUtils.sha256Hex(ts + "adsk-" + tenant);
        System.out.println("hs_ts ---> " + ts);
        System.out.println("hs_secret ---> " + secret);
//        httpPost.addHeader("hs_ts",ts);
//        httpPost.addHeader("hs_secret",secret);
    }
}
