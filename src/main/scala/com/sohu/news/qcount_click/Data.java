package com.sohu.news.qcount_click;

import java.io.Serializable;

public class Data implements Serializable {
    int count;
    long logTime;
    Data(int count,long logTime){
        this.count=count;
        this.logTime=logTime;
    }
}
