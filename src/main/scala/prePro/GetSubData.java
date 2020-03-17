package prePro;

import java.io.*;

public class GetSubData {
    public static void main(String args[]) throws Exception{
        String loadFilepath="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\HIGGSDIs.csv";
        String writeFilepath1="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\sub1.csv";
        String writeFilepath2="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\sub2.csv";
        String writeFilepath3="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\sub3.csv";
        String writeFilepath4="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\sub4.csv";
        String writeFilepath5="C:\\Users\\Administrator\\Desktop\\全部实验数据集\\sub5.csv";
        File writeFile1=new File(writeFilepath1);writeFile1.createNewFile();
        File writeFile2=new File(writeFilepath2);writeFile2.createNewFile();
        File writeFile3=new File(writeFilepath3);writeFile3.createNewFile();
        File writeFile4=new File(writeFilepath4);writeFile4.createNewFile();
        File writeFile5=new File(writeFilepath5);writeFile5.createNewFile();

        BufferedWriter out1=new BufferedWriter(new FileWriter(writeFile1));
        BufferedWriter out2=new BufferedWriter(new FileWriter(writeFile2));
        BufferedWriter out3=new BufferedWriter(new FileWriter(writeFile3));
        BufferedWriter out4=new BufferedWriter(new FileWriter(writeFile4));
        BufferedWriter out5=new BufferedWriter(new FileWriter(writeFile5));
        BufferedReader loadedFile=loadFile(loadFilepath);
        String curLine=loadedFile.readLine();
        String title=addTitle(curLine.split(",").length);
        System.out.println(title);
        out1.write(title);
        out2.write(title);
        out3.write(title);
        out4.write(title);
        out5.write(title);

        int totalLineNum=11000000;
        int sub1Num=totalLineNum/40;
        int sub2Num=totalLineNum/4;
        int sub3Num=totalLineNum/2;
        int sub4Num=3*totalLineNum/4;

        curLine=loadedFile.readLine()+"\r\n";
        System.out.println(curLine);

        for(int i=0;i<totalLineNum-1;i++){
            if(i<sub1Num)
                out1.write(curLine);
            if(i<sub2Num)
                out2.write(curLine);
            if(i<sub3Num)
                out3.write(curLine);
            if(i<sub4Num)
                out4.write(curLine);
            curLine=loadedFile.readLine()+"\r\n";
           // System.out.println(curLine);
        }
        out1.close();
        out2.close();
        out3.close();
        out4.close();
        out5.close();

    }

    public static BufferedReader loadFile(String filepath) throws java.io.FileNotFoundException{
        File solvFile=new File(filepath);
        InputStreamReader isReader=new InputStreamReader(
                new FileInputStream(solvFile));
        BufferedReader bf=new BufferedReader(isReader);
        return bf;
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
