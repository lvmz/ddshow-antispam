package com.youku.ddshow.antispam.model.QianKunDai;

import java.io.Serializable;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class ErrorMessage implements Serializable {

    private  int code;
    private  String  desc;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
