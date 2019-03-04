package com.hadoop.hdfs.mr.rackaware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

public class MyRackAware  implements DNSToSwitchMapping {
    public List<String> resolve(List<String> names) {
        List<String> list = new ArrayList<String>();
        for(String name:names){
            //输出原来的信息，ip地址（主机名）
            System.out.println("==========================>>"+name);
            if(name.startsWith("192")){
                //192.168.230.201
                String ip = name.substring(name.lastIndexOf("."));
                if(Integer.parseInt(ip)<=203){//202,203为一个机架
                    list.add("/rack1/"+ip);
                }else{//204,205为一个机架
                    list.add("/rack2/"+ip);
                }
            }else if(name.startsWith("s")){
                String ip = name.substring(1);
                if(Integer.parseInt(ip)<=203){//202,203为一个机架
                    list.add("/rack1/"+ip);
                }else{//204,205为一个机架
                    list.add("/rack2/"+ip);
                }
            }

        }

        return list;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> list) {

    }
}
