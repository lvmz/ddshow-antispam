package com.youku.ddshow.antispam.model.QianKunDai;

import java.util.List;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class QkdMgetResult extends BaseResult {
    private int cost;
    private List<Entity> ks;

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public List<Entity> getKs() {
        return ks;
    }

    public void setKs(List<Entity> ks) {
        this.ks = ks;
    }
}
