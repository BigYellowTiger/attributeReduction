package Kernel_2019_6_11;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class Test {
    public static void main(String[] args) throws Exception{
        StringBuffer a=new StringBuffer("123");
        StringBuffer b=a;
        b.append("456");
        System.out.println(a);
    }


}

class T{
    int a;
    public T(int aaa){
        a=aaa;
    }

    static synchronized void printC() throws Exception{
        System.out.println("c");
        Thread.sleep(0);
    }

    public synchronized void printA() throws Exception{
        System.out.println("a");
        Thread.sleep(3000);
    }

    public synchronized void printB() throws Exception{
        System.out.println("b");
        Thread.sleep(3000);
    }
}

class B extends T{
    int a;
    public B(int ccc){
        super(ccc);
        a=133;
    }
}
