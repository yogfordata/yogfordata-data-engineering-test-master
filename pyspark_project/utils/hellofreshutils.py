import __main__


from pyspark.sql.functions import regexp_replace,split,col,round
from pyspark.sql.types import  DoubleType



def cookTimeMinute(timestr):
  newtm=0
  if timestr:
    newtm=0
    if timestr.replace("PT","").replace("H","").replace("M",""):
      newtm=0
      hour=0

      newtimestr=timestr.replace("PT","").replace("M","")
      if 'H' in timestr :
        val=newtimestr.split("H")
        if val[1]:
          newtm = int(val[0])*60+int(val[1])
        else:
          newtm = int(val[0])*60

      else:
        newtm=int(newtimestr)


  return newtm
