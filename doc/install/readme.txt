参考
http://blog.csdn.net/dabokele/article/details/52235657
http://azkaban.github.io/azkaban/docs/2.5/#executor-setup
http://www.cnblogs.com/tannerBG/archive/2014/07/10/3835952.html
一、前言
最近试着参照官方文档搭建 Azkaban，发现文档很多地方有坑，所以在此记录一下。
二、环境及软件
需要软件：
azkaban-web-server-2.5.0.tar.gz
https://s3.amazonaws.com/azkaban2/azkaban2/2.5.0/azkaban-web-server-2.5.0.tar.gz
azkaban-executor-server-2.5.0.tar.gz
https://s3.amazonaws.com/azkaban2/azkaban2/2.5.0/azkaban-executor-server-2.5.0.tar.gz
azkaban-sql-script-2.5.0.tar.gz
https://s3.amazonaws.com/azkaban2/azkaban2/2.5.0/azkaban-sql-script-2.5.0.tar.gz
Azkaban Plugins
Job Types Plugins azkaban-jobtype-2.5.0.tar.gz
https://s3.amazonaws.com/azkaban2/azkaban-plugins/2.5.0/azkaban-jobtype-2.5.0.tar.gz
HDFS Browser azkaban-hdfs-viewer-2.5.0.tar.gz
https://s3.amazonaws.com/azkaban2/azkaban-plugins/2.5.0/azkaban-hdfs-viewer-2.5.0.tar.gz
Azkaban Security Manager azkaban-hadoopsecuritymanager-2.5.0.jar
https://s3.amazonaws.com/azkaban2/azkaban-plugins/2.5.0/azkaban-hadoopsecuritymanager-2.5.0.tar.gz

Azkaban source： github.com/azkaban/azkaban
Azkaban plugins source：github.com/azkaban/azkaban-plugins
doc：azkaban.github.io/azkaban/docs/2.5/

三、配置Mysql
解压azkaban-sql-script-2.5.0.tar.gz
mysql -uroot -proot
create database azkaban;
use azkaban;
source create-all-sql-2.5.0.sql;
grant all privileges on azkaban.* to 'azkaban'@'localhost' identified by 'azkaban';
flush privileges;

四、配置 azkaban-web
1,解压 azkaban-web-server-2.5.0.tar.gz
2,生成SSL 证书
关于怎么使用 Java keytool 生成 keystore 和 Truststore 文件 可以参考我之前的随笔。
http://www.cnblogs.com/tannerBG/p/3834093.html
	1、生成一个含有一个私钥的keystore文件
		keytool -genkey -keystore keystore -alias jetty-azkaban -keyalg RSA
		Enter keystore password:
		Re-enter new password:
		What is your first and last name?
		  [Unknown]:  azkaban
		What is the name of your organizational unit?
		  [Unknown]:  Jetty
		What is the name of your organization?
		  [Unknown]:  Aug
		What is the name of your City or Locality?
		  [Unknown]:  SH
		What is the name of your State or Province?
		  [Unknown]:  SH
		What is the two-letter country code for this unit?
		  [Unknown]:  86
		Is CN=azkaban, OU=Jetty, O=Aug, L=SH, ST=SH, C=86 correct?
		  [no]:  yes
		Enter key password for <jetty-azkaban2>
		(RETURN if same as keystore password):
	2、验证生成的keystore文件
		keytool -list -v -keystore keystore.jks
		Enter keystore password:
	3、导出凭证文件
		keytool -export -alias jetty-azkaban -keystore keystore.jks -rfc -file selfsignedcert.cer
	    Enter keystore password:
	4,导入认凭证件cer文件到truststore文件
		keytool -import -alias certificatekey -file selfsignedcert.cer -keystore truststore.jks
	    Enter keystore password:
	5,查看生成的truststore文件
		keytool -list -v -keystore truststore.jks

	在这里可以只简单的生成 keystore 文件，并将生成的 keystore 文件拷贝至 /usr/local/ae/azkaban/azkaban-web-2.5.0 文件下。
	本文中证书文件为 keystone， keypass 为 password。

五、配置 azkaban-executor
解压 azkaban-executor-server-2.5.0.tar.gz











