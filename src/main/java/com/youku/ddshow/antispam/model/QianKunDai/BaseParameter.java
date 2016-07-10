package com.youku.ddshow.antispam.model.QianKunDai;

import java.io.Serializable;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class BaseParameter implements Serializable {
    private String uri;

    private String ak;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getAk() {
        return ak;
    }

    public void setAk(String ak) {
        this.ak = ak;
    }
}
