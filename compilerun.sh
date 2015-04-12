if sbt/sbt compile package assembly; then
	echo "Compile OK" 1>&2
else
	echo "Compile ERROR: Aborting." 1>&2
	exit 1
fi

if ~/spark-1.3.0/bin/spark-submit --class edu.gatech.cse8803.main.Main --master "local[*]" target/scala-2.10/cse8803_project-assembly-1.0.jar $1 $2 $3 $4; then
	echo "Run OK" 1>&2
else
	echo "Run ERROR: Aborting." 1>&2
	exit 1
fi