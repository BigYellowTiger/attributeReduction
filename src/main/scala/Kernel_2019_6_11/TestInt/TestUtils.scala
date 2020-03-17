package Kernel_2019_6_11.TestInt

object TestUtils {
  def searchFun(index:Integer):(Int,Int)=>Int={
    (a,b)=>{
      if(a==1)
        throw new RuntimeException(index.toString)
      else
        b
    }
  }

  def searchFun2(index:Integer):(Int,Int)=>Int={
    (a,b)=>{
      if(a==1 && index%2==0)
        throw new RuntimeException(index.toString)
      else
        b
    }
  }

  def searchFun3(index:Integer):(Int,Int)=>Int={
    (a,b)=>{
      if(a==1 && index%3==0)
        throw new RuntimeException(index.toString)
      else
        b
    }
  }
}
