package com.sohu.news.util.common;

public class StringUtils {
    public StringUtils() {
    }

    public static boolean isBlank(String str) {
        return str == null || str.trim().equals("");
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String getStringWithVal(String val, String defaultval) {
        return isBlank(val)?defaultval:val;
    }

    public static void main(String[] args) {
        System.out.printf("asmndkasj");
    }
}