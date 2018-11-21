requirements:
- python2.7.5
- hadoop2.8.0


如何运行：
1、打开hadoop:
在master机器上，进入hadoop的sbin目录，输入 ./start-all.sh 启动hadoop   
`cd /usr/local/hadoop/hadoop-2.8.0/sbin/`   
`./start-all.sh`

2、假定输入文件是hdfs中/input/LICENSE.txt，在本文件夹下执行下列命令：
`hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.8.0.jar \    
-input /input/LICENSE.txt \   
-output /output/wordcount \   
-mapper mapper.py \   
-reducer reducer.py \   
-file mapper.py \   
-file reducer.py`

3、查看结果：
`hadoop fs -cat /output/wordcount/part-00000`
