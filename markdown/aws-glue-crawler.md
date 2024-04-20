# AWS Glue Crawler
> Jay Kumsi
## Glue Crawler
  * A Program that connects to your data source (S3 ,DyDB) to scan your data and creates metadata tables in Glue Data Catalog
  * Can scan multuple data sources in single run
  * Once completed,it will create table in Data catalog
  * This table can be used in Athena,ETL jobs,Redshift Spectrum

  ![image](https://github.com/jaykumsi/aws-glue/assets/137452836/133157d9-b3fc-4716-b7b0-6d7c3ed06863)
  
 ## Data Sources that Glue Crawlers can crawl
  * Native client
    * S3:
    * Dynamo DB:
  * JDBC 
      * Amazon Redshift
      * Snowflake
      * Amazon Aurora
      * MariaDB
      * Microsoft SQL Server
      * MySQL
      * Oracle
      * PostgreSQL
  * MangoDB client 
      * Mongo DB
      * MongoDB Atlas
      * Amazon DocumentDB

* IAM Roles
 * Initially, it is necessary to establish an AWS Identity and Access Management (IAM) policy before proceeding to create an IAM 
   role.
    
 * Go to IAM (Identity and Access Management) 
  1. click the policies tab on the left side of the page
  2. click the Create policy
 ![Alt text](images/image-2.png)

 * Permissions : click on Json tab and paste the below 
                 code and Click Next
![Alt text](images/image-3.png)
 ```json
 {
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject"
			],
			"Resource": "arn:aws:s3:::tini-d-gluebucket-001*"
		}
	]
}
```
* Policy Name : Enter a Policy Name accordingly and click 
                create policy to create a policy
 ![Alt text](images/image-4.png)
 ![Alt text](images/image-5.png)

* Below Json policy is for Glue Service Role
```json
{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": [
					"glue:*",
					"s3:GetBucketLocation",
					"s3:ListBucket",
					"s3:ListAllMyBuckets",
					"s3:GetBucketAcl",
					"iam:ListRolePolicies",
					"iam:GetRole",
					"iam:GetRolePolicy",
					"cloudwatch:PutMetricData"
				],
				"Resource": [
					"*"
				]
			},
			{
				"Effect": "Allow",
				"Action": [
					"s3:GetObject",
					"s3:PutObject"
				],
				"Resource": [
					"arn:aws:s3:::aws-glue-*/*",
					"arn:aws:s3:::*/*aws-glue-*/*"
				]
			},
			{
				"Effect": "Allow",
				"Action": [
					"s3:GetObject"
				],
				"Resource": [
					"arn:aws:s3:::crawler-public*",
					"arn:aws:s3:::aws-glue-*"
				]
			},
			{
				"Effect": "Allow",
				"Action": [
					"logs:CreateLogGroup",
					"logs:CreateLogStream",
					"logs:PutLogEvents"
				],
				"Resource": [
					"arn:aws:logs:*:*:*:/aws-glue/*"
				]
			}
		]
	}
```

## Create Crawler Roles for the policies created
 * Go to IAM (Identity and Access Management) , click the 
   Roles tab on the left side of the page and click on create role

 ![Alt text](images/image-6.png)
 
    1. select the trusted entity as below image, for trusted entity type, select AWS service 
    2. For Use case, select Glue
    3. Click Next

 ![Alt text](images/image-8.png)

  * Add Permissions 
   1. select the policies created for S3
   2. Select the policies created for Glue 
   3. Click Next

   ![Alt text](images/image-9.png)

  *  Role Name 
   1. The policy for S3
   2. The policy for Glue are attached 
   3. Enter a Unique Role name 
   4. Click Next.

   ![Alt text](images/image-10.png)


## Glue Catalog
  * Go to AWS Glue, click the crawlers tab on the left   
   side of the page ,click create crawler.
   1. Click Crawler tab
   2. Click Create Crawler
    ![Alt text](images/image-11.png)
 
 * Set Crawler properties
   1. Name :: Enter Unique Name for the Crawler
   2. Click Next
    ![Alt text](images/image-12.png)   

  * Choose the data source and classifiers,Data source configuration , Add a 
   data source.
   1. Select S3 as a Data Source
   2. Select S3 bucket path
   3. crawl all the folders(it will crawl all subfolder, you choose according to your requirement)
   4. Add a Data Source


  ![Alt text](images/image-13.png)
  ![Alt text](images/image-14.png)
 
  * Configure Security Setting
   1. Choose the existing IAM role for crawler
   2. Click Next
   ![Alt text](images/image-15.png)

  * Set Output and Scheduling
   1. Select the Target Database, we have already created glue_db . 
   2. If Database is not created then click Add Database to create one.
   3. select the frequency on which the crawler to run. 
   4. Click next
   ![Alt text](images/image-16.png)

  * Crawler Schedule, you can select the crawler schedule  
    on how to run it, below you can choose anyone.
    ![Alt text](images/image-18.png)
     
  * Once you select scheduler, then click next and review 
   the full setting before clicking finish. It will create a crawler to move the data from S3 to Data 	 
   Catalog
   ![Alt text](images/image-19.png)  

  * Once the Crawler is created, please select Run 
    Crawler and once it is completed it will display as below.
  1. Click Crawler.
  2. Start time , this will display the start time for the  
      crawler.
  3. End time, this will display the end time for the crawler.
  4. Status , this will display the status of the crawler job.
  
   ![Alt text](images/image-20.png)

  * Crawler S3
    * `Source` CSV `Target` CSV
    * `Source` JSON `Target` JSON
    * `Source` Parquet `Target` Parquet
    * `Source` ION `Target` ION
        * New File [Do this with AWS Lambda]
        * Add Rows [Do this with AWS Lambda]
        * Remove Rows [Do this with AWS Lambda]
           
* Crawler DB
    * Crawler On single tables
    * Crawler On PK-FK tables
    * Crawler On View
* Crawler Dynamo
* Crawler Metrics (Stats like DB Tables, last crawled datetime, PCT scanned, Pending time for Crawl)
* Crawler performance tuning
=======
# AWS Glue Crawler
> Jay Kumsi
## Glue Crawler
  * A Program that connects to your data source (S3 ,DyDB) to scan your data and creates metadata tables in Glue Data Catalog
  * Can scan multuple data sources in single run
  * Once completed,it will create table in Data catalog
  * This table can be used in Athena,ETL jobs,Redshift Spectrum

  ![image](https://github.com/jaykumsi/aws-glue/assets/137452836/133157d9-b3fc-4716-b7b0-6d7c3ed06863)
  
 ## Data Sources that Glue Crawlers can crawl
  * Native client
    * S3:
    * Dynamo DB:
  * JDBC 
      * Amazon Redshift
      * Snowflake
      * Amazon Aurora
      * MariaDB
      * Microsoft SQL Server
      * MySQL
      * Oracle
      * PostgreSQL
  * MangoDB client 
      * Mongo DB
      * MongoDB Atlas
      * Amazon DocumentDB

* IAM Roles
 * Initially, it is necessary to establish an AWS Identity and Access Management (IAM) policy before proceeding to create an IAM 
   role.
    
 * Go to IAM (Identity and Access Management) 
  1. click the policies tab on the left side of the page
  2. click the Create policy
 ![Alt text](images/image-2.png)

 * Permissions : click on Json tab and paste the below 
                 code and Click Next
![Alt text](images/image-3.png)
 ```json
 {
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject"
			],
			"Resource": "arn:aws:s3:::tini-d-gluebucket-001*"
		}
	]
}
```
* Policy Name : Enter a Policy Name accordingly and click 
                create policy to create a policy
 ![Alt text](images/image-4.png)
 ![Alt text](images/image-5.png)

* Below Json policy is for Glue Service Role
```json
{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": [
					"glue:*",
					"s3:GetBucketLocation",
					"s3:ListBucket",
					"s3:ListAllMyBuckets",
					"s3:GetBucketAcl",
					"iam:ListRolePolicies",
					"iam:GetRole",
					"iam:GetRolePolicy",
					"cloudwatch:PutMetricData"
				],
				"Resource": [
					"*"
				]
			},
			{
				"Effect": "Allow",
				"Action": [
					"s3:GetObject",
					"s3:PutObject"
				],
				"Resource": [
					"arn:aws:s3:::aws-glue-*/*",
					"arn:aws:s3:::*/*aws-glue-*/*"
				]
			},
			{
				"Effect": "Allow",
				"Action": [
					"s3:GetObject"
				],
				"Resource": [
					"arn:aws:s3:::crawler-public*",
					"arn:aws:s3:::aws-glue-*"
				]
			},
			{
				"Effect": "Allow",
				"Action": [
					"logs:CreateLogGroup",
					"logs:CreateLogStream",
					"logs:PutLogEvents"
				],
				"Resource": [
					"arn:aws:logs:*:*:*:/aws-glue/*"
				]
			}
		]
	}
```

## Create Crawler Roles for the policies created
 * Go to IAM (Identity and Access Management) , click the 
   Roles tab on the left side of the page and click on create role

 ![Alt text](images/image-6.png)
 
    1. select the trusted entity as below image, for trusted entity type, select AWS service 
    2. For Use case, select Glue
    3. Click Next

 ![Alt text](images/image-8.png)

  * Add Permissions 
   1. select the policies created for S3
   2. Select the policies created for Glue 
   3. Click Next

   ![Alt text](images/image-9.png)

  *  Role Name 
   1. The policy for S3
   2. The policy for Glue are attached 
   3. Enter a Unique Role name 
   4. Click Next.

   ![Alt text](images/image-10.png)


## Glue Catalog
  * Go to AWS Glue, click the crawlers tab on the left   
   side of the page ,click create crawler.
   1. Click Crawler tab
   2. Click Create Crawler
    ![Alt text](images/image-11.png)
 
 * Set Crawler properties
   1. Name :: Enter Unique Name for the Crawler
   2. Click Next
    ![Alt text](images/image-12.png)   

  * Choose the data source and classifiers,Data source configuration , Add a 
   data source.
   1. Select S3 as a Data Source
   2. Select S3 bucket path
   3. crawl all the folders(it will crawl all subfolder, you choose according to your requirement)
   4. Add a Data Source


  ![Alt text](images/image-13.png)
  ![Alt text](images/image-14.png)
 
  * Configure Security Setting
   1. Choose the existing IAM role for crawler
   2. Click Next
   ![Alt text](images/image-15.png)

  * Set Output and Scheduling
   1. Select the Target Database, we have already created glue_db . 
   2. If Database is not created then click Add Database to create one.
   3. select the frequency on which the crawler to run. 
   4. Click next
   ![Alt text](images/image-16.png)

  * Crawler Schedule, you can select the crawler schedule  
    on how to run it, below you can choose anyone.
    ![Alt text](images/image-18.png)
     
  * Once you select scheduler, then click next and review 
   the full setting before clicking finish. It will create a crawler to move the data from S3 to Data 	 
   Catalog
   ![Alt text](images/image-19.png)  

  * Once the Crawler is created, please select Run 
    Crawler and once it is completed it will display as below.
  1. Click Crawler.
  2. Start time , this will display the start time for the  
      crawler.
  3. End time, this will display the end time for the crawler.
  4. Status , this will display the status of the crawler job.
  
   ![Alt text](images/image-20.png)

  * Crawler S3
    * `Source` CSV `Target` CSV
    * `Source` JSON `Target` JSON
    * `Source` Parquet `Target` Parquet
    * `Source` ION `Target` ION
        * New File [Do this with AWS Lambda]
        * Add Rows [Do this with AWS Lambda]
        * Remove Rows [Do this with AWS Lambda]
           
* Crawler DB
    * Crawler On single tables
    * Crawler On PK-FK tables
    * Crawler On View
* Crawler Dynamo
* Crawler Metrics (Stats like DB Tables, last crawled datetime, PCT scanned, Pending time for Crawl)
* Crawler performance tuning
>>>>>>> 38ff75ddde0a9bd3c09c78d3b88f592404b6c95c
