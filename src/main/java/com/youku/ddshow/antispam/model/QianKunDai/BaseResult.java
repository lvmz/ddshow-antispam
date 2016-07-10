package com.youku.ddshow.antispam.model.QianKunDai;

import java.io.Serializable;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class BaseResult implements Serializable {
    private ErrorMessage e;

    public ErrorMessage getE() {
        return e;
    }

    public void setE(ErrorMessage e) {
        this.e = e;
    }
}
