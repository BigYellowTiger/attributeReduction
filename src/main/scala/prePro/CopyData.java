package prePro;

import java.io.*;

//用于复制数据
public class CopyData {
    public static void main(String args[]) throws Exception{
        String loadFilepath="E:\\迅雷下载\\SVHN\\mushroomsTransfer.csv";
        String writeFilepath="E:\\迅雷下载\\SVHN\\5000copymushuroom.csv";
        File writeFile=new File(writeFilepath);writeFile.createNewFile();
        BufferedWriter out=new BufferedWriter(new FileWriter(writeFile));
        BufferedReader loadedFile=loadFile(loadFilepath);
        String title=loadedFile.readLine()+"\r\n";
        System.out.println(title);
        out.write(title);
        String curLine=loadedFile.readLine();
        System.out.println(curLine);
        int repeatTime=5000;
        while (curLine!=null){
            for(int i=0;i<repeatTime;i++){
                out.write(curLine+"\r\n");
            }
            curLine=loadedFile.readLine();
        }
        out.close();
    }

    public static BufferedReader loadFile(String filepath) throws java.io.FileNotFoundException{
        File solvFile=new File(filepath);
        InputStreamReader isReader=new InputStreamReader(
                new FileInputStream(solvFile));
        BufferedReader bf=new BufferedReader(isReader);
        return bf;
    }
}
