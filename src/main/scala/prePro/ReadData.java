package prePro;

import scala.collection.Map;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


//将数据文件离散化
//每个属性用逗号分隔开，最后一个为标签
public class ReadData {
    public static void main(String[] args)throws Exception{
        long startTime = System.currentTimeMillis();
        //String loadFilepath="C:\\Users\\Administrator\\Desktop\\testMax.csv";
        String loadFilepath="C:\\Users\\qly\\Desktop\\DS1.csv";
        String writeFilepath="C:\\Users\\Administrator\\Desktop\\bigData\\adfsas.csv";
        File writeFile=new File(writeFilepath);
        File solveFile=new File(loadFilepath);
        InputStreamReader isReader=new InputStreamReader(new FileInputStream(solveFile));
        BufferedReader bf=new BufferedReader(isReader);
        HashMap<Integer,String> saveMap=new HashMap<>();
        List<String> ls=new ArrayList<>();

        int lineNum=0;
        //writeFile.createNewFile();
        //BufferedWriter out=new BufferedWriter(new FileWriter(writeFile));
        BufferedReader loadedFile=bf;
        //loadedFile.mark((int)solveFile.length()+100000);
            //read title
        String curr="";
        while(curr!=null){
            curr=loadedFile.readLine();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");


    }

    //离散化
    public static String discreLine(String line,Double[] step,Double[] min){
        String[] temp=line.split(",");
        String result="";
        for(int i=0;i<step.length;i++){
            int cache=(int)((Double.valueOf(temp[i])-min[i])/step[i]);
            result+=String.valueOf(cache);
            if(i!=step.length-1)
                result+=",";
        }
        result+="\r\n";
        return result;
    }

    //加载文件
    public static BufferedReader loadFile(String filepath) throws FileNotFoundException{
        File solvFile=new File(filepath);
        InputStreamReader isReader=new InputStreamReader(
                new FileInputStream(solvFile));
        BufferedReader bf=new BufferedReader(isReader);
        return bf;
    }

    //读取文件
    public static String readFileLineToString(BufferedReader bf) throws IOException{
        return bf.readLine();
    }

    //获取并记录最大最小值
    public static void recordMaxAndMin(String line,Double[] max,Double[] min,int titleLength){
        String[] temp=line.split(",");
        for(int i=0;i<temp.length;i++){
            if(Double.valueOf(temp[i])>max[i])
                max[i]=Double.valueOf(temp[i]);
            if(Double.valueOf(temp[i])<min[i])
                min[i]=Double.valueOf(temp[i]);
        }
        int a=0;
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

