# television
MapReduce Job for television.txt (https://drive.google.com/file/d/0Bxr27gVaXO5sVjQ5QW0wQ3RCTUU/view) Dataset is sales of different TV sets across different locations. The Description of data set attached in television.txt is as follows: - 1st Column - Company Name 2nd Column - Product Name 3rd Column - Size in inches 4th Column - State 5th Column - Pin Code 6th Column - Price

There are some invalid records which contain "NA"  in either Company Name and Product Name

## Task 1
Write a Map-Reduce program to filter out the invalid records.  

### Solution-
1. Filter all the CompanyName and ProductName which contain NA.
2. Print all other CompanyName and ProductName.
3. Checked for NA using If Conditional. 
4. Make use of Counter to calculate INVALID_RECORD_COUNT.
5. First I checked if all the hadoop daemons were running.
6. I found History Server was not running , so I started it using command "mr-jobhistory-daemon.sh start historyserver"
7. Then I checked for my jar file. It was in the Desktop "my_fourth.jar".
8. Then I saved my sample data for job i.e. "television.txt" into hdfs.
9. I executed my job with command "hadoop jar /home/acadgild/Desktop/my_fourth.jar /television.txt /adi1".
10. When the job completed successfully,I checked the adi1 with command "hadoop dfs -ls /adi1"
11. It showed 2 files namely SUCCESS and part-r-00000.
12. I checked the result stored in part-r-00000 using "hadoop dfs -cat /adi1/part-r-00000".

### OUTPUT-
It showed the result as -
Akai      Decent
Lava      Attention
Lava      Attention
Lava      Attention
Onida     Decent
Onida     Lucent
Onida     Lucent
Samsung   Super
Samsung   Super
Samsung   Super
Samsung   Decent
Samsung   Optima
Samsung   Optima
Samsung   Optima
Zen       Super
Zen       Super
 

## Task 2
Write a Map-Reduce program to calculate the total units sold for each Company.

### Solution-
1. Filter all the CompanyName and ProductName which contain NA.
2. Calculate the number of units sold for each CompanyName .
3. Make use of Counter to calculate INVALID_RECORD_COUNT.
4. The result is CompanyName and Number of units sold.
5. First I checked if all the hadoop daemons were running.
6. I found History Server was not running , so I started it using command "mr-jobhistory-daemon.sh start historyserver"
7. Then I checked for my jar file. It was in the  "/home/acadgild/task2.jar".
8. Then I saved my sample data for job i.e. "television.txt" into hdfs.
9. I executed my job with command "hadoop jar /home/acadgild/task2.jar /input/television.txt /ou5".
10. When the job completed successfully,I checked the adi1 with command "hadoop dfs -ls /ou5"
11. It showed 2 files namely SUCCESS and part-r-00000.
12. I checked the result stored in part-r-00000 using "hadoop dfs -cat /ou5/part-r-00000".

### OUTPUT-
It showed the result as- 
Akai        1
Lava        3
Onida       3
Samsung     7
Zen         2
 

## Task 3
Write a Map-Reduce program to calculate the total units sold in each state for Onida Company.

### Solution-
1. Filter all the CompanyName and ProductName which contain NA.
2. Store State of those record whose CompanyName is Onida.
3. Print State and Number of units sold.
4. Make use of Counter to calculate INVALID_RECORD_COUNT.
5. First I checked if all the hadoop daemons were running.
6. I found History Server was not running , so I started it using command "mr-jobhistory-daemon.sh start historyserver"
7. Then I checked for my jar file. It was in the  "/home/acadgild/wer.jar".
8. Then I saved my sample data for job i.e. "/input/television.txt" into hdfs.
9. I executed my job with command "hadoop jar /home/acadgild/wer.jar /input/television.txt /ou55".
10. When the job completed successfully,I checked the adi1 with command "hadoop dfs -ls /ou55"
11. It showed 2 files namely SUCCESS and part-r-00000.
12. I checked the result stored in part-r-00000 using "hadoop dfs -cat /ou55/part-r-00000".

### OUTPUT-
It showed the result as- 
Uttar Pradesh       1
Uttar Pradesh       1
Uttar Pradesh       1
 
