# Azkaban安装文档 #
作者：段云涛<duanyuntao@sinovatech.com><br>
版本：1.0<br>
修订日期：2015/1/30<br>

----------

## 1、其他软件准备 ##

	1. Hadoop 2.5.2
	2. Java 1.7.0
	3. Hive 0.14.0
	4. Mysql 5.1.63
	5. Pig 0.12.0
## 2、环境需求 ##
	1. Hadoop集群 - 保证可用，并配置了相关的环境变量，包括：JAVA_HOME、HADOOP_HOME、HADOOP_MAPRED_HOME、HADOOP_COMMON_HOME、HADOOP_HDFS_HOME、HADOOP_YARN_HOME、HADOOP_CONF_DIR。
	2. Hive - 在服务器上已经部署，并配置HIVE_HOME环境变量，将$HIVE_HOME/bin和$HADOOP_HOME/bin加入到环境变量PATH中。
	3. MySQL Server可用
## 3、准备工作 ##
### 3.1、下载azkaban发行版本安装包 ###
	下载地址：http://azkaban.github.io/downloads.html
	1. azkaban-web-server-2.5.0.tar.gz
	2. azkaban-executor-server-2.5.0.tar.gz
	3. azkaban-sql-script-2.5.0.tar.gz
	4. azkaban-jobtype-2.5.0.tar.gz
	5. azkaban-hdfs-viewer-2.5.0.tar.gz
	6. azkaban-jobsummary-2.5.0.tar.gz
	7. azkaban-reportal-2.5.0.tar.gz

### 3.2、下载azkaban plugins源码 ###
	安装过程中需要修改一些bug，重新编译azkaban plugins。<br>
	下载地址：https://github.com/azkaban/azkaban-plugins，并且选择对应的版本进行下载。
### 3.3、约定 ###
	1. 本文中所有azkaban组件安装在一台服务器上，安装根目录为：/app/azkaban（下文中将用${appRoot}来引用）。

## 4、安装azkaban ##
### 4.1、MySQL数据库的初始化 ###
目前 Azkaban 只支持 MySql ，故需安装 MySql 服务器，安装 MySql 的过程这里不作介绍。

	命令：
	1. 创建数据库（该数据库只供azkaban使用）
		CREATE DATABASE azkaban;
	2. 创建azkaban用户，并设置密码。
		CREATE USER 'azkaban'@'%' IDENTIFIED BY 'azkaban';
	3. 给予azkaban用户在azkaban数据库的所有权限。
		GRANT ALL ON azkaban.* to 'azkaban'@'%' WITH GRANT OPTION; 
	4. 解压缩 azkaban-sql-2.5.0.tar.gz文件，并进入到 azkaban-sql-script目录，然后进入 mysql 命令行模式。
		mysql -uazkaban -pazkaban
		use azkaban
		source create-all-sql-2.5.0.sql
### 4.2、安装 Azkaban Web Server ###
	1. 将azkaban-web-server-2.5.0.tar.gz放入${appRoot}目录。
		cp azkaban-web-server-2.5.0.tar.gz ${appRoot}
	2. 解压缩azkaban-web-server-2.5.0.tar.gz
		tar -zxvf azkaban-web-server-2.5.0.tar.gz
	3. 生成 keystore 证书文件,保证Keystore放在${appRoot}/azkaban-web-2.5.0目录下。
		cd ${appRoot}/azkaban-web-2.5.0
		keytool -keystore keystore -alias jetty -genkey -keyalg RSA
	4. 修改web-server配置文件azkaban.properties
		cd ${appRoot}/azkaban-web-2.5.0/conf
		vim azkaban.properties
		1. 修改时区和首页名称
		azkaban.name=Sinovatech
		azkaban.label=Sinovatech-Azkaban
		azkaban.color=#FF3601
		azkaban.default.servlet.path=/index
		web.resource.dir=web/
		default.timezone.id=Asia/Shanghai
		viewer.plugin.dir=plugins/viewer
		2. 用户和权限管理
		user.manager.class=azkaban.user.XmlUserManager
		user.manager.xml.file=conf/azkaban-users.xml
		3. MySQL配置
		database.type=mysql
		mysql.port=3306
		mysql.host=10.40.33.11
		mysql.database=azkaban
		mysql.user=azkaban
		mysql.password=azkaban
		mysql.numconnections=100
		4. Jetty配置，将前面生成Keystore时候的配置加入以下配置
		jetty.keystore=keystore
		jetty.password=azkaban
		jetty.keypassword=azkaban
		jetty.truststore=keystore
		jetty.trustpassword=azkaban
		jetty.hostname=10.40.33.11
		jetty.maxThreads=25
		jetty.ssl.port=8443
		jetty.port=8081
		#same as azkaban.execution.dir
		working.dir=/home/sinova/bin/jars/executions
### 4.3、安装 azkaban-executor-server ###
	1. 将azkaban-executor-server-2.5.0.tar.gz放入${appRoot}目录。
		cp azkaban-executor-server-2.5.0.tar.gz ${appRoot}
	2. 解压缩azkaban-executor-server-2.5.0.tar.gz
		tar -zxvf azkaban-executor-server-2.5.0.tar.gz
	3. 修改配置文件azkaban.properties
		cd ${appRoot}/azkaban-executor-2.5.0/conf
		vim azkaban.properties
		1. 修改时区
		default.timezone.id=Asia/Shanghai
		2. 指定jobtypes目录
		azkaban.jobtype.plugin.dir=plugins/jobtypes
		3. MySQL配置
		database.type=mysql
		mysql.port=3306
		mysql.host=10.40.33.11
		mysql.database=azkaban
		mysql.user=azkaban
		mysql.password=azkaban
		mysql.numconnections=100
### 4.4、插件安装 ###
#### 4.4.1、安装HDFS Viewer插件 ####
	1. 将azkaban-hdfs-viewer-2.5.0.tar.gz放入${appRoot}/azkaban-web-2.5.0/plugins/viewer目录。
		cp azkaban-hdfs-viewer-2.5.0.tar.gz {appRoot}/azkaban-web-2.5.0/plugins/viewer
	2. 解压缩azkaban-hdfs-viewer-2.5.0.tar.gz
		tar -zxvf azkaban-hdfs-viewer-2.5.0.tar.gz
	3. 重命名文件夹为hdfs。
		mv azkaban-hdfs-viewer-2.5.0 hdfs
	4. 配置HDFS Viewer
		cd ${appRoot}/azkaban-web-2.5.0/plugins/viewer/hdfs/conf
		vim plugin.properties
		修改以下配置
		viewer.name=HDFS
		viewer.path=hdfs
		viewer.order=1
		viewer.hidden=false
		viewer.external.classpaths=extlib/*
		viewer.servlet.class=azkaban.viewer.hdfs.HdfsBrowserServlet
		hadoop.security.manager.class=azkaban.security.HadoopSecurityManager_H_2_0
		azkaban.should.proxy=false
		proxy.user=azkaban
		proxy.keytab.location=
		allow.group.proxy=false
		file.max.lines=1000
	5. 添加以下（hadoop）jar文件到${appRoot}/azkaban-web-2.5.0/plugins/viewer/hdfs/extlib
		commons-cli-1.2.jar
		hadoop-auth-2.5.2.jar
		hadoop-common-2.5.2.jar
		hadoop-hdfs-2.5.2.jar
		hadoop-mapreduce-client-core-2.5.2.jar
		protobuf-java-2.5.0.jar
#### 4.4.2、安装Job Summary插件 ####
	1. 将azkaban-jobsummary-2.5.0.tar.gz放入${appRoot}/azkaban-web-2.5.0/plugins/viewer目录。
		cp azkaban-jobsummary-2.5.0.tar.gz {appRoot}/azkaban-web-2.5.0/plugins/viewer
	2. 解压缩azkaban-jobsummary-2.5.0.tar.gz
		tar -zxvf azkaban-jobsummary-2.5.0.tar.gz
	3. 重命名文件夹为jobsummary。
		mv azkaban-jobsummary-2.5.0 jobsummary
#### 4.4.3、安装Reportal插件 ####
Reportal插件的安装不仅是要在Azkaban Web Server上进行（Viewer插件），也需要在Azkaban Executor Server上进行（Jobtype插件）。

	1. 由于azkaban-reportal-2.5.0.tar.gz中不仅包含Viewer插件，还包含Jobtype插件，所以先在一个临时目录中解压这个包，然后将解压得到的viewer/reportal/目录拷贝到上步中的viewer目录下。最终这个插件的目录路径为：
	{appRoot}/azkaban-web-2.5.0/plugins/viewer/reportal。
	2. 修改{appRoot}/azkaban-web-2.5.0/plugins/viewer/reportal/conf/plugin.properties。由于Web Server和Executor Server是分开部署，不能使用本地文件存储report任务的结果，而是用hdfs存储：
	reportal.output.filesystem=hdfs
#### 4.4.4、安装Jobtypes插件 ####
	1. 将azkaban-jobtype-2.5.0.tar.gz放入${appRoot}/azkaban-executor-2.5.0/plugins目录。
		cp azkaban-jobtype-2.5.0.tar.gz {appRoot}/azkaban-executor-2.5.0/plugins
	2. 解压缩azkaban-jobtype-2.5.0.tar.gz
		tar -zxvf azkaban-jobtype-2.5.0.tar.gz
	3. 重命名文件夹为jobtypes。
		mv azkaban-jobtype-2.5.0 jobtypes
	4. 将4.4.2中解压得到reportaldatacollector、reportalhive、reportalpig、reportalteradata放入jobtypes文件夹中。
	5. 修改commonprivate.properties和common.properties加入hive.home、hadoop.home、pig.home
	6. 进入 {appRoot}/azkaban-executor-2.5.0/plugins/jobtypes目录建立pig软连接
		ls -n pig-0.12.0 pig
### 4.5、 bug修复 ###
	1. 将准备好的azkaban-jobtype-2.5.jar、azkaban-hadoopsecuritymanager-yarn-2.5.jar、azkaban-reportal-2.5.jar 、azkaban-hadoopsecuritymanager-2.5.jar替换以下目录中对应的-2.5.0.jar。
		1. ${appRoot}/azkaban-web-2.5.0/plugins/viewer/hdfs/lib
		2. ${appRoot}/azkaban-web-2.5.0/plugins/viewer/jobsummary/lib
		3. ${appRoot}/azkaban-web-2.5.0/plugins/viewer/reportal/lib
		4. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/hadoopJava
		5. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/hive
		6. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/java
		7. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/pig
		8. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/reportaldatacollector
		9. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/reportalhive
		10. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/reportalpig
		11. ${appRoot}/azkaban-executor-2.5.0/plugins/jobtypes/reportalteradata
	2. 将已经修复完的azkaban-2.5.jar替换以下目录中对应的-2.5.0.jar
		1.  ${appRoot}/azkaban-web-2.5.0/lib
		2.  ${appRoot}/azkaban-executor-2.5.0/lib
### 4.6、用户和权限设置 ###
	cd ${appRoot}/azkaban-web-2.5.0/conf
	vim azkaban-users.xml
	配置信息如下：
		<azkaban-users>
	        <user username="azkaban" password="azkaban" roles="admin" groups="azkaban" />
	        <user username="metrics" password="metrics" roles="metrics"/>
	        <user username="sinova" password="sinova" roles="admin" groups="azkaban" />
	
	        <role name="admin" permissions="ADMIN" />
	        <role name="metrics" permissions="METRICS"/>
	        <role name="readall" permissions="READ"/>
	        <role name="writeall" permissions="WRITE"/>
	        <role name="executeall" permissions="EXECUTE"/>
	        <role name="scheduleall" permissions="SCHEDULE"/>
	        <role name="createprojects" permissions="CREATEPROJECTS"/>
		</azkaban-users>
### 4.7、azkaban的启动和停止 ###
	1. 启动web-server
		cd ${appRoot}/azkaban-web-2.5.0
		bin/start-web.sh
	2. 停止web-server
		bin/azkaban-web-shutdown.sh
	3. 启动exe-server
		cd ${appRoot}/azkaban-executor-2.5.0
		bin/start-exec.sh
	4. 停止exe-server
		bin/azkaban-executor-shutdown.sh