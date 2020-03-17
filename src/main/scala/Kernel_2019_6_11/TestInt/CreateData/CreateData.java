package Kernel_2019_6_11.TestInt.CreateData;

import java.io.*;
public class CreateData {
    public static void main(String[] a) throws Exception{
        String writeFilepath="C:\\Users\\qly\\Desktop\\allone.csv";
        File writeFile=new File(writeFilepath);
        BufferedWriter out=new BufferedWriter(new FileWriter(writeFile));
        for(int i=0;i<100;i++){
            out.write("1,1,1,1,1,1,1\r\n");
        }
        out.flush();
        out.close();
    }

}
