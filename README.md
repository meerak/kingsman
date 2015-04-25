#BDH 8803: Project - Patient Similarity Using GraphX

####Dependencies: 
The data( vocabularies, exact) in OMOP-CDM format needs to be downloaded into the data folder. 
Since these files are quite large, we will not be including it in the zip file.

Exact - https://drive.google.com/file/d/0BxcFMDUYO1ItMnZDelMtaTVSVTQ/view?usp=sharing

Vocabularies  - https://drive.google.com/file/d/0BxcFMDUYO1ItY1JTdWZWM0JpQTg/view?usp=sharing

PostgreSQL driver - Can be found in dependency folder

####Compile: 
We use sbt to compile the code. 

> sbt compile 

Since our code depends on some other packages, we need to create an uber jar. We create this using the assembly plugin in sbt.

> sbt assembly

####Run: 
The application can be launched using the bin/spark-submit script available in spark folder.

The general format is:
```./bin/spark-submit \
  --class <main-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]```

To run in local mode:

```./spark-submit --class edu.gatech.cse8803.main.Main --master local --driver-memory 5G  /Users/rhea/rheahome/Spring2015/BDH8803/Project/target/scala-2.10/cse8803_project_template-assembly-1.0.jar```

To run in yarn-client:

```./spark-submit --class edu.gatech.cse8803.main.Main --master yarn-client --driver-memory 10G --executor-memory 10G --jars /home/hadoop/code/postgresql-9.3-1103.jdbc41.jar /home/hadoop/code/target/scala-2.10/cse8803_project_template-assembly-1.0.jar ```

More details on the arguments can be found on the official Spark site: http://spark.apache.org/docs/1.2.0/

To run KNN:
To compute the AUC for mortality prediction

Call testKNN_AUC function with the graph as its input. 'casecontrol.txt' holds all the patient ID's contained in the cohort. The function will read from this text file and create a list: knnvertices. For every vertex inside this list, its mortality probability is computed using the knnOneVsAll function. The similairty metric of choice can be passed as input here. This function will return output in the format '(actual death label, predicted value of death label)' for every patient in the cohort.

The auc.py function uses the roc plotting function provided by the scikit-learn library. This function accepts as input a text file comprising of the output generated by the testKNN_AUC function. The ROC curve will be plotted as output.

####Deploy:
To deploy on EMR, please follow the instructions:

1. When you setup the EMR cluster, set the bootstrap action to custom action,
and specify the location as
s3://support.elasticmapreduce/spark/install-spark

2. Once the cluster is ready, SSH into the master node:
ssh -i <keypair location> hadoop@<master-dns-address>
Create two folders in the master  - data and code 
mkdir data
mkdir code

3. Add a user to master node
sudo useradd rhea
sudo passwd rhea

4. Install PostgreSQL on master node
sudo yum install postgresql postgresql-server
sudo service postgresql initdb

5. Configure PostgreSQL on master node
  1. Change authentication models for local to trust.
  2. Change authentication for any host i.e. 0.0.0.0/0 to md5
  3. Change port and listen_address
  ```bash
  sudo su postgres 
  cd /var/lib/pgsql9/data
  vim pg_hba.conf
  local   all         all                                  trust
  host    all         all         0.0.0.0/0          md5
  
  vim postgresql.conf
  listen_addresses = '*' 
  port = 5432

  exit
  ``` 
6. Start PostgreSQL
sudo service postgresql start -p 5432

7. Transfer data files from local machine to remote master node
  ```bash
  scp -i <keypair> data/exactnew.sql hadoop@<master-dns-address>:/home/hadoop/data
  scp -i <keypair> data/vocab.sql hadoop@<master-dns-address>:/home/hadoop/data 
  ```

8. Create databases and load data on master node
  ```bash
  sudo su postgres
  createdb vocab
  createdb exact
  psql -d vocab
  
  vocab=# CREATE USER rhea with password 'abcde';
  CREATE ROLE
  vocab=# GRANT ALL PRIVILEGES ON DATABASE vocab to rhea;
  GRANT
  \q
  
  psql -d exact
  GRANT ALL PRIVILEGES ON DATABASE exact to rhea;
  \q 
  
  exit
  
  psql -d exact -f data/exactnew.sql -U rhea
  psql -d vocab -f data/vocab.sql -U rhea
  ```
9. Transfer source code from local machine to remote master node
 ```bash
  scp -i <keypair> -r src hadoop@<master-dns-address>:/home/hadoop/code
  scp -i <keypair> -r sbt hadoop@<master-dns-address>:/home/hadoop/code
  scp -i <keypair> build.sbt hadoop@<master-dns-address>:/home/hadoop/code
  scp -i <keypair> project/build.properties hadoop@<master-dns-address>:/home/hadoop/code/project
  scp -i <keypair> project/plugins.sbt hadoop@<master-dns-address>:/home/hadoop/code/project
  scp -i <keypair> dependency/postgresql-9.3-1103.jdbc41.jar hadoop@<master-dns-address>:/home/hadoop/code
  ```
  
10. Change host in application.conf in resource to use master's ip address
 ```bash
 vim src/main/resources/application.conf
  #Change host from localhost, to ip-address of master node 
  ```
  
11. Create the uber jar
 ```bash
  sbt compile assembly
  ```

12. Run the code
 ```bash
  ./spark-submit --class edu.gatech.cse8803.main.Main --master yarn-client --driver-memory 10G --executor-memory 10G --executor-cores 2 --num-executors 4 --jars /home/hadoop/code/postgresql-9.3-1103.jdbc41.jar /home/hadoop/code/target/scala-2.10/cse8803_project_template-assembly-1.0.jar
  ```

