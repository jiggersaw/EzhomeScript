package com.company;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Wormholes on 2016/1/2.
 */
public class MDHexUtil {

    private static char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

    public static String toHex2(byte[] data) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int d = data[i];
            if (d < 0) d += 256;
            if (d < 16) buf.append("0");
            buf.append(Integer.toHexString(d));
        }
        return buf.toString();
    }

    public static String getMD5Hex(byte[] b) throws NoSuchAlgorithmException {
        MessageDigest mdInst = MessageDigest.getInstance("MD5");
        mdInst.update(b);
        byte[] md = mdInst.digest();
        return toHex(md);
    }

    public static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (int i = 0; i < b.length; i++) {
            sb.append(hexDigits[(b[i] & 0xf0) >>> 4]);
            sb.append(hexDigits[b[i] & 0x0f]);
        }
        return sb.toString().toUpperCase();
    }
}
