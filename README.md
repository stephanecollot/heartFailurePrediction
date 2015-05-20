
# Abstract
### Objective :
Our objective in this paper is to be able to automatically predict whether or not a given 
patient is likely to suffer from heart failure (Congestive Heart Failure) in the near future given his 
medical history.  
### Methods :
To achieve this, we use medical records of more than 10,000 patients from the ExactData 
dataset  to  construct  features  based  on  Diagnoses,  Risk  Factors,  Medication  History,  and 
Laboratory Test history in a five-year window. Then, we train classifiers using ensemble machine 
learning methods, mainly bagging with logistic regression and random forests. We then use k-fold 
cross-validation to evaluate performance and choose classifier parameters using the ML pipeline 
framework in Spark.  
### Results :
We were able to achieve an accuracy of over 98% in a testing set consisting of 50% of 
individuals who did not suffer from this disease and 50% of individuals who were diagnosed with 
heart failure, while maintaining a low false negative rate.  
### Conclusion :
We have excellent results with the confusion matrix; we sit back and discuss them by 
considering potential bias from the data set.   

# Instructions
### Dependencies :
```
Spark 1.3.0
Spark MLlib
Spark ML(High level API for machine learning pipelines)
Scala
sbt
Amazon Web Service
```

### Compile & Run :

I) Navigate to cse8803 folder

II) Put the big data set in ```data/GeorgiaTech_DS1_CSV/```

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

### Deploy:
If you want to launch it on a AWS cluster:
```
~/spark/bin/spark-submit --class edu.gatech.cse8803.main.Main --master "spark://ec2-52-1-230-43.compute-1.amazonaws.com:7077"   --deploy-mode cluster  target/scala-2.10/cse8803_project-assembly-1.0.jar big
```
