BDH 8803: Project - Patient Similarity Using GraphX

Dependencies: list all dependencies and give install instructions if it's not trivial.  
The data( vocabularies, exact) in OMOP-CDM format needs to be downloaded. 
Since these files are quite large, we will not be including it in the zip file.
Exact - https://drive.google.com/file/d/0BxcFMDUYO1ItMnZDelMtaTVSVTQ/view?usp=sharing
Vocabularies  - https://drive.google.com/file/d/0BxcFMDUYO1ItY1JTdWZWM0JpQTg/view?usp=sharing

PostgreSQL driver - Can be found in dependency folder

Compile: describe how to compile the code, for example using tools like make, sbt, mvn, gradle or ant.
We use sbt to compile the code. 

> sbt compile 

Since our code depends on some other packages, we need to create an uber jar. We create this using the assembly plugin in sbt.

> sbt assembly

Run: how to run your promgram. what parameters? what input format is required?
The application can be launched using the bin/spark-submit script available in spark folder.

The general format is:
./bin/spark-submit \
  --class <main-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]

To run in local mode:
./spark-submit --class edu.gatech.cse8803.main.Main --master local --driver-memory 5G  /Users/rhea/rheahome/Spring2015/BDH8803/Project/target/scala-2.10/cse8803_project_template-assembly-1.0.jar

To run in yarn-client:
./spark-submit --class edu.gatech.cse8803.main.Main --master yarn-client --driver-memory 10G --executor-memory 10G --jars /home/hadoop/code/postgresql-9.3-1103.jdbc41.jar /home/hadoop/code/target/scala-2.10/cse8803_project_template-assembly-1.0.jar

More details on the arguments can be found on the official Spark site: http://spark.apache.org/docs/1.2.0/

Deploy:
To deploy on EMR, please refer to the instructions in docs/EMRInstructions.txt
