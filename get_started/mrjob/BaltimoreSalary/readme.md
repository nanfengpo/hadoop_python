
# 执行
## 一、本地运行

    $ python top_salary.py Baltimore_City_Employee_Salaries_FY2014.csv >output.txt 2>error.txt

## 二、Hadoop运行
1、自动上传输入文件，直接运行：

    $ python top_salary.py -r hadoop Baltimore_City_Employee_Salaries_FY2014.csv 
    $ python avg_salary.py -r hadoop Baltimore_City_Employee_Salaries_FY2014.csv 

2、手动上传输入文件，然后运行：

    $ hadoop fs -copyFromLocal /home/nanfengpo/Documents/hadoop_get_started/mrjob/BaltimoreSalary/Baltimore_City_Employee_Salaries_FY2014.csv /input
    
    $ python top_salary.py -r hadoop hdfs:///input/Baltimore_City_Employee_Salaries_FY2014.csv 
    $ python avg_salary.py -r hadoop hdfs:///input/Baltimore_City_Employee_Salaries_FY2014.csv 

