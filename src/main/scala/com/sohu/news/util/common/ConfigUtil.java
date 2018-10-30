package com.sohu.news.util.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by huijiehuai210249 on 2017/4/1.
 */
public class ConfigUtil implements Serializable{
    Properties properties = new Properties();
    public String getString(String key){
        return properties.getProperty(key);
    }
    public int getInt(String key){
        return Integer.parseInt(properties.getProperty(key));
    }

    public void load(String path){
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(path);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }finally {
            assert (inputStream != null);
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static ConfigUtil single = new ConfigUtil();
    static {
        single.load("config.properties");
    }
    public static ConfigUtil getConfig() {
        return single;
    }

    public static void main(String[] args) {
        String profile = ConfigUtil.getConfig().getString("mvn.profile");
        System.out.println("self profile is " + profile);
    }
}
