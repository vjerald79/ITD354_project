hadoop fs -rmdir /flume/sink/wind_temp_rf
hadoop fs -mkdir /flume/sink/wind_temp_rf
hadoop fs -copyFromLocal /home/flume/itd354project/output/Rainfall_Wind_Temp.csv /flume/sink/wind_temp_rf 

