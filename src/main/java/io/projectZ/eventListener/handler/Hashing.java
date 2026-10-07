package io.projectZ.eventListener.handler;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 10/2/2026 - 6:50 PM
*/

import java.security.MessageDigest;
import java.util.Objects;

public class Hashing {
    public static String hash(String s1, String s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);

        String first, second;
        if (s1.compareTo(s2) <= 0) {
            first = s1;
            second = s2;
        } else {
            first = s2;
            second = s1;
        }

        String combined = first.length() + ":" + first + "|" + second.length() + ":" + second;

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(combined.getBytes("UTF-8"));
            StringBuilder hex = new StringBuilder();
            for (byte b : hashBytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.substring(0,25);
        } catch (Exception e) {
            throw new RuntimeException("hash calculation failed", e);
        }
    }
}

