package prePro;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


//将数据文件转换为csv文件
//每个属性用逗号分隔开，最后一个为标签
public class EpsilonTransfer {
    public static void main(String[] args)throws Exception{
        String loadFilepath="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\DS3.csv";
        String writeFilepath="C:\\Users\\Administrator\\Desktop\\bigData\\dfadsfasfs.csv";
        File writeFile=new File(writeFilepath);
        try {
            int lineNum=0;
            //边读边写
            writeFile.createNewFile();
            BufferedWriter out=new BufferedWriter(new FileWriter(writeFile));
            BufferedReader loadedFile=loadFile(loadFilepath);
            //String test=readFileLineToString(loadedFile);
            //System.out.println(solveLineToString(test.split(" ")));
            String curLine=readFileLineToString(loadedFile);
            int counter1=0;

            while (curLine!=null){
                System.out.println(curLine);
                curLine=loadedFile.readLine();
                //System.out.println(curLine);
                counter1++;
            }
            System.out.print(counter1);
            String title=addTitle(curLine.split(" ").length);
            System.out.println(curLine);
            System.out.println(title);
            int standarLength=curLine.split(" ").length;
            out.write(title);

            while (curLine!=null){
                if(curLine.split(" ").length==standarLength){
                    curLine=solveLineToString(curLine.split(" "));
                    //System.out.println(curLine);
                    out.write(curLine);
                    lineNum++;
                }
                curLine=readFileLineToString(loadedFile);
            }
            out.close();
            System.out.println("line num="+lineNum);
        }catch (Exception e){}

    }

    //加载文件
    public static BufferedReader loadFile(String filepath) throws java.io.FileNotFoundException{
        File solvFile=new File(filepath);
        InputStreamReader isReader=new InputStreamReader(
                new FileInputStream(solvFile));
        BufferedReader bf=new BufferedReader(isReader);
        return bf;
    }

    //读取文件
    public static String readFileLineToString(BufferedReader bf) throws java.io.IOException{
        return bf.readLine();
    }

    //对每一行进行处理，输出加好逗号的行
    public static String solveLineToString(String[] inputLine){
        String result="";
        String finalString="";
        for(int i=0;i<inputLine.length;i++){
            StringBuffer solvingLine=new StringBuffer(inputLine[i]);
//            if(solvingLine.indexOf(":")!=-1){
//                //保留“：”以后的字符串
//                int startIndex=solvingLine.indexOf(":")+1;
//                String cache=solvingLine.substring(startIndex);
//                result+=cache;
//                result+=",";
//            }
            /**针对mushroom的特别版*/
            if(solvingLine.indexOf(":")!=-1){
                //保留“：”以后的字符串
                int endIndex=solvingLine.indexOf(":");
                String cache=solvingLine.substring(0,endIndex);
                result+=cache;
                result+=",";
            }
            /*******************/
            else {
                finalString=inputLine[i];
            }
        }
        result+=finalString;
        result+="\r\n";
        return result;
    }

    //添加标题栏
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
