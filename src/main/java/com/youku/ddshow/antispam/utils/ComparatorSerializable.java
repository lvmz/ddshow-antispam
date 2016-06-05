package com.youku.ddshow.antispam.utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by dongjian on 2016/6/1.
 */
public class ComparatorSerializable<T> implements Serializable, Comparator {
    public int compare(Long obj1, Long obj2) {
        // 降序排序
        return obj2.compareTo(obj1);
    }

    @Override
    public int compare(Object o1, Object o2) {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }
}
