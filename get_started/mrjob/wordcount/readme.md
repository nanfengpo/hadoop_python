requirements:
- python2.7.5
- hadoop2.8.0
- mrjob

运行：
1、打开hadoop:
在master机器上，进入hadoop的sbin目录，输入 ./start-all.sh 启动hadoop
# cd /usr/local/hadoop/hadoop-2.8.0/sbin/
# ./start-all.sh

2、先在命令行中测试：
# python wordcount_mrjob.py input.txt
结果为标准输出

3、在hadoop中运行：
# python wordcount_mrjob.py -r hadoop input.txt

使用mrjob的优点在于不需要先把输入文件传送到hdfs中，会自动传进去！
如果想处理hdfs中的文件，则需要使用hdfs:///的地址：
python wordcount_mrjob.py -r hadoop hdfs:///input/LICENSE.txt
