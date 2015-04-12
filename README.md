# cse8803

## Compile & Run :

```
sbt/sbt compile
sbt/sbt run
sbt/sbt "run big"
```
On Windows:
```
sbt compile
sbt run
sbt/sbt "run big"
```

## Compile & Run Bis

```
sbt/sbt compile package assembly
~/spark/bin/spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar
~/spark/bin/spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar big
```
On Windows:
```
sbt compile  package assembly
C:\spark\bin\spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar
C:\spark\bin\spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar big
```

## Data
Put the big data set in ```data/GeorgiaTech_DS1_CSV/```.

Clean the big data set: execute ```python27 regex.py encounter.csv``` then replace ```encounter.csv``` with ```regexOut.csv```
