package com.hadoop.hdfs.mr.maxTemp.allsort.secondarysort;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearGroupComparator extends WritableComparator {

    protected  YearGroupComparator(){
        super(ComboKey.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey comboKey01 = (ComboKey)a;
        ComboKey comboKey02 = (ComboKey)b;
        return comboKey01.getYear() - comboKey02.getYear();
    }

}
