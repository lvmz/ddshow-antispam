package com.youku.ddshow.antispam.model;

import java.io.Serializable;

/**
 * Created by dongjian on 2016/6/21.
 */
public class UgcLog   implements Serializable {
    private  String ip;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    private  String token;


}
