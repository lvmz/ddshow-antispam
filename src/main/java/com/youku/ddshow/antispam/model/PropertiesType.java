package com.youku.ddshow.antispam.model;

/**
 * Created by kouzhigang on 2015/12/2.
 */
public enum PropertiesType {
    DDSHOW_STAT_TEST("druid_ddshow_stat_test.properties"),
    DDSHOW_STAT_ONLINE("druid_ddshow_stat_online.properties"),
    CONTROL_STAT("druid_control_stat.properties"),
    DDSHOW_PLATFORM_ONLINE("druid_ddshow_platform_online.properties"),
    DDSHOW_PLATFORM_TEST("druid_ddshow_platform_test.properties"),
    DDSHOW_HASE_TEST("ddshow_hbase_test"),
    DDSHOW_HASE_ONLINE("ddshow_hbase_online");

    private String value;

    public String getValue() {
        return value;
    }

    PropertiesType(String value) {
        this.value = value;
    }
}
