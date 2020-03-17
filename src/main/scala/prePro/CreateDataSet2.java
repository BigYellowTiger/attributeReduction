package prePro;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class CreateDataSet2 {
    public static void main(String[] haoleiaaaaaaaaa)throws Exception{
        //Random r=new Random();
        ThreadLocalRandom r=ThreadLocalRandom.current();
        int features=10001;
        int lineNum=100000;
        String writeFilepath="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\DS4.csv";
        File writeFile=new File(writeFilepath);
        writeFile.createNewFile();
        BufferedWriter out=new BufferedWriter(new FileWriter(writeFile));

        String title=addTitle(features);
        out.write(title);
        long startTime=new Date().getTime();
        for(int i=0;i<lineNum;i++){
            String temp="";
            for(int j=0;j<features;j++){
                temp+=r.nextInt(10);
                if(j!=features-1)
                    temp+=",";
            }
            temp+="\r\n";
            out.write(temp);
        }
        out.close();
        long endTime=new Date().getTime();
        System.out.println(endTime-startTime);
    }

    public static String addTitle(int attNum){
        String result="";
        String finalString="decideAtt";
        for(int i=1;i<attNum;i++){
            result+="att"+i+",";
        }
        result+=finalString;
        result+="\r\n";
        return result;
    }
}
