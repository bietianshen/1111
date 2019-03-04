package com.hadoop.hdfs.mr;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Util {


    public static String getInfo(Object o,String msg){
        return getHostname() +":"+getPid()+":"+getTid()+":"+getObjId(o)+":"+msg;


    }

    /**
     * 获取主机名
     */
    public static String getHostname(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取进程id
     */
    public static String getPid(){
        RuntimeMXBean rr = ManagementFactory.getRuntimeMXBean();
        String pidAndhostname = rr.getName();//pid@hostname
        String pid = pidAndhostname.substring(0,pidAndhostname.indexOf("@"));
        return pid;
    }

    /**
     * 获取进程名称
     */
    public static String getTid(){
        return Thread.currentThread().getName();
    }

    /**
     * 获取对象名称
     */
    public static String getObjId(Object o){
        String obj = o.getClass().getSimpleName();
        return obj +"@"+  o.hashCode();
    }






}
