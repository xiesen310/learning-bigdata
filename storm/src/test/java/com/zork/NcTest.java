package com.zork;

import junit.framework.TestCase;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 16:13 2018/2/13
 */
public class NcTest extends TestCase {

    public void test() throws Exception {
        // 得到runtime对象
        Runtime r = Runtime.getRuntime();
        // 执行命令
        Process p = r.exec("nc localhost 8888");
        OutputStream os = null;
        // 获取输出流
        os = p.getOutputStream();
        for (int i = 0; i < 10; i++) {
            os.write(("hello world" + i + "\n").getBytes());
        }
        os.close();
    }
}
