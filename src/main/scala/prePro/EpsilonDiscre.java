package prePro;

import java.io.*;


//将数据文件离散化
//每个属性用逗号分隔开，最后一个为标签
public class EpsilonDiscre {
    public static void main(String[] args)throws Exception{
        //String loadFilepath="C:\\Users\\Administrator\\Desktop\\testMax.csv";
        String loadFilepath="C:\\Users\\Administrator\\Desktop\\bigData\\HIGGS.csv";
        String writeFilepath="C:\\Users\\Administrator\\Desktop\\bigData\\adfsas.csv";
        File writeFile=new File(writeFilepath);
        File solveFile=new File(loadFilepath);
        InputStreamReader isReader=new InputStreamReader(new FileInputStream(solveFile));
        BufferedReader bf=new BufferedReader(isReader);
        try {
            int lineNum=0;
            writeFile.createNewFile();
            BufferedWriter out=new BufferedWriter(new FileWriter(writeFile));
            BufferedReader loadedFile=bf;
            //loadedFile.mark((int)solveFile.length()+100000);

            //read title
            String firstLine=loadedFile.readLine();
            String title=firstLine+"\r\n";

            System.out.println(firstLine);
            System.out.println(title);

            int titleLength=title.split(",").length;
            int discrenum=10;
            Double[] max=new Double[titleLength];
            Double[] min=new Double[titleLength];
            Double[] step=new Double[titleLength];
            for(int i=0;i<titleLength;i++){
                max[i]=-10000000.0;
                min[i]= 10000000.0;
                step[i]=0.0;
            }
            System.out.println(max[0]);
            out.write(title);
            //record max and min
            String curLine=loadedFile.readLine();
            System.out.println(curLine);
            while(curLine!=null){
                //System.out.println(curLine);
                recordMaxAndMin(curLine,max,min,titleLength);
                lineNum++;
                //System.out.print(1);
                curLine=loadedFile.readLine();
            }
            System.out.println();
            System.out.println("find max and min");

            for(int i=0;i<titleLength;i++){
                step[i]=(max[i]*1.001-min[i])/discrenum;
            }

            System.out.println("step0="+step[0]);

            System.out.println("second cal");
            File solveFile2=new File(loadFilepath);
            InputStreamReader isReader2=new InputStreamReader(new FileInputStream(solveFile2));
            BufferedReader bf2=new BufferedReader(isReader2);
            title=bf2.readLine();
            System.out.println("Second:"+title);
            curLine=bf2.readLine();

            lineNum=0;
            while (curLine!=null){
                //System.out.println(curLine);
                String writeLine=discreLine(curLine,step,min);
                //System.out.println(writeLine);
                out.write(writeLine);
                curLine=bf2.readLine();
                lineNum++;
            }
            out.close();

            System.out.println("line num="+lineNum);
        }catch (Exception e){
            e.printStackTrace();
        }

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
