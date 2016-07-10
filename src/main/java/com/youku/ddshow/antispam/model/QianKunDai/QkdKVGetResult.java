package com.youku.ddshow.antispam.model.QianKunDai;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class QkdKVGetResult extends  BaseResult{
    private int cost;
    private  String k;
    private  String v;

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }
}
