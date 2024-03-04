# AWS Glue
> Jay Kumsi

## Glue Topics
* Intro 
* What IAM Roles are needed

### Glue with PySpark
#### Spark with various data sources
* S3
* Athena
* EC2 DB, Files
* Dynamo
* RDS
* SNS
* SQS
* AirFlow

#### Glue Spark DynamicFrames and DataFrames
* Glue Dynamic Frames
* Glue PySpark DataFrame Joins (Inner, Left, Right, Full Outer)
* Glue PySpark DataFrame Group By
* Glue PySpark DataFrame Analytical Functions
* Glue PySpark DataFrame Order By
* Glue PySpark DataFrame Having Clause
* Glue PySpark DataFrame Filter Conditions IN, NOT IN, >,<,<>
* Glue PySpark DataFrame Distinct, Psuedo Colums, Date, DateTime, DateTime TimeZone
* Glue PySpark DataFrame Handling: Null, isnull, not null

### Glue with Crawler
* [Crawler](markdown/aws-glue-crawler.md)
    * Source
    * Target
    * Source S3 (CSV,Parquet,JSON,AWS ION) - Target(S3,RDBMS,DynamoDB)
        * IAM roles needed
        * Code Structure
    * Source RDBMS (Oracle, SQL Server, Postgresql) - Target(S3,RDBMS,  
                    DynamoDB) 
    * Source Dynamo - Target(S3,RDBMS,DynamoDB)
    * Create Script to crawl the S3 Zip file to Data Catalog
    * Create a log group using the script
    * Add above crawl to the Log group
    * Read errors from latest log in the specified log group
    * Partitioning through crawler 
    * partitioning through Athena
    * Inserting through Athena
    * Serde to read parquet/JSon/CSV/Ion


## GITHUB Deployment with Actions
* Working with GitHub
* AWS CLI with Glue-spark
    * Create Job
    * Update Job
    * Run Job
* AWS CLI with Glue-crawler
    * Create Job
    * Update Job
    * Run Job
* Deploy code from github to AWS account
    * Settings
    * actions>yaml file
    * On Push, On Merge


