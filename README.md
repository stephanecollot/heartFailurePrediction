
# cse8803 

## Dependencies :
```
Spark 1.3.0
Spark MLlib
Spark ML(High level API for machine learning pipelines)
Scala
sbt
Amazon Web Service
```

## Compile & Run :

I) Navigate to cse8803 folder

II) Put the big data set in ```data/GeorgiaTech_DS1_CSV/```.

III) Clean the big data set: 

	1) execute ```python27 regex.py encounter.csv``` then replace ```encounter.csv``` with ```regexOut.csv```
	
	2) execute ```python27 regex.py encounter_outpatient.csv``` then replace ```encounter_outpatient.csv``` with ```regexOut.csv```

IV) Execute
```
sbt/sbt compile package assembly
~/spark/bin/spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar big
```
On Windows:
```
sbt compile  package assembly
C:\spark\bin\spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar big
```

## Deploy:
```
```