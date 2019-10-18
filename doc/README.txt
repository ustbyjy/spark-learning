# 编译源码
./dev/make-distribution.sh --name 2.6.0-cdh5.7.0 --tgz -Pyarn -Phadoop-2.6 -Phive -Phive-thriftserver -Dhadoop.version=2.6.0-cdh5.7.0

# 编译CDH版本的需要在pom.xml文件中加cloudera仓库
<repository>
	<id>cloudera</id>
	<url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
</repository>

# 提交作业
spark-submit \
--name SQLContextApp
--class com.yjy.spark.sql.SQLContextApp \
--master local[2] \
/root/spark/app/spark-sql-1.0.jar \
/root/spark/examples/src/main/resources/people.json