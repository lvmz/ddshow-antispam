package com.youku.ddshow.antispam.model.QianKunDai;

import java.util.List;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class QkdMgetParameter extends BaseParameter {
    private List<Key> ks;
    public List<Key> getKs() {
        return ks;
    }

    public void setKs(List<Key> ks) {
        this.ks = ks;
    }
}
