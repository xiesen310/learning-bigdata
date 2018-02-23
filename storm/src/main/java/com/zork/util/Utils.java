package com.zork.util;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Author: xiesen
 * Description: 工具类
 * Date: Created in 16:38 2018/2/13
 */
public class Utils {
    public static OutputStream os = null;

    static {
        Runtime rr = Runtime.getRuntime();
        try {
            Process p = rr.exec("nc localhost 8888");
            os = p.getOutputStream();
        } catch (Exception e) {
        }
    }

    /**
     * 输出日志到nc
     * @param o
     * @param msg
     */
    public static void outLog2NC(Object o, String msg) {
        try {
            String prefix = "";
            // 获取系统时间
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
            String time = sdf.format(date);

            // 获取主机名
            String hostName = InetAddress.getLocalHost().getHostName();

            // pid
            RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();
            // 8808@master
            String pid = r.getName().split("@")[0];

            // Thread
            String tname = Thread.currentThread().getName();
            long tid = Thread.currentThread().getId();
            String tinfo = tname + "-" + tid;

            String oclass = o.getClass().getSimpleName();
            int ohash = o.hashCode();
            String oinfo = oclass + "@" + ohash;
            prefix = "[" + time + " " + hostName + " " + pid + " " + tinfo + " " + oinfo + "] " + msg + "\n";

            os.write(prefix.getBytes());
            os.flush();
//            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Utils.outLog2NC(new Random(), " Hello");
    }
}
