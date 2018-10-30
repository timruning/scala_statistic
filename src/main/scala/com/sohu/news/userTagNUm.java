package com.sohu.news;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class userTagNUm {
    public static void main(String[] args) {
        String path = "/opt/develop/workspace/sohu/news/data/2018-08-10";
        BufferedReader reader = null;
        int num = 0;
        try {
            reader = new BufferedReader(new FileReader(path));
            String line = null;
            while ((line = reader.readLine()) != null) {
                try {
                    JSONObject jsonObject = new JSONObject(line);
                    JSONArray jsonArrayK = jsonObject.getJSONArray("keywords1.0");
                    Boolean k = jsonArrayK != null && jsonArrayK.length() > 0;
                    JSONArray jsonArrayV = jsonObject.getJSONArray("v1.1_video");
                    Boolean v = jsonArrayV != null && jsonArrayV.length() > 0;
                    JSONArray jsonArrayT = jsonObject.getJSONArray("v1.1_text");
                    Boolean t = jsonArrayT != null && jsonArrayT.length() > 0;
                    if (k || v || t) {
                        num += 1;
                    } else {
                        System.out.println(line);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        System.out.println(num);
    }
}
