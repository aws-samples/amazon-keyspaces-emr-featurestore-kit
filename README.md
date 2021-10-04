# Using Amazon Keyspaces and Delta Lake on S3 to build a Feature Store
To train Machine Learning models we need historical data. During this phase the Data Scientists experiment with different features to test which ones produce the best model. From a platform perspective, we need to support bulk read and write operations. Read latency is not critical at this stage since the data is read into training jobs. Once the models are trained and moved to Production for real-time inference, the requirements for the platform change: we need to support low latency reads and only the latest features data is used.

This repo accompaines a blogpost which discusses how to build a Feature Store.[link]. 

## Setup the Dataset 
This repo contains the dataset we need to place in an S3 bucket. We will use Smart meters in London datase, you can find it under Data/daily_dataset.csv. This dataset consists of smart energy meter readings which were rolled out to homes in the UK. The file daily_dataset.csv contains information like the number of measures, minimum, maximum, mean, median, sum and std for each household on a daily basis. To create an S3 bucket (if you don’t already have one) and upload the data file follow these steps:
1.	Clone the project repository locally by running the shell command
git clone [repo url pending]
2.	Log into the console and navigate to S3
3.	Select Create Bucket
4.	Give the bucket a name, we called ours “featurestore-blogpost-bucket-xxxxxxxxxx” (its handy to append account number to bucket name to ensure the name is unique for common prefixes)
5.	Select the region you are working in. It is important that you create all resources in the same region for this blogpost
6.	Public access is blocked by default, and we recommend that you keep it that way.
7.	We disable bucket versioning and encryption since we don’t need it. 
8.	Click Create bucket
9.	Once the bucket is created, click on the bucket name and drag the folder Data into the bucket

## Setup Keyspaces
We need to generate credentials for Keyspaces which will be used to connect with the service. The steps for generating the credentials are as follows:
1.	Log into the console and navigate to IAM
2.	Select Users, choose an IAM user you want to generate the Keyspaces credentials for
3.	In the user menu navigate to the tab Security credentials
4.	Scroll down to Credentials for Amazon Keyspaces (for Apache Cassandra) and select Generate Credentials
5.	This will create a pop-up with the credentials, and an option to download the credentials. We recommend downloading a copy since you wont be able to view the credentials again
We also need to create a table in Keyspaces which will store our feature data. We have shared the schema for the keyspace and table in the github project files Keyspaces/keyspace.sql and Keyspaces/Table_Schema.sql. To create this table follow the steps:
1.	Log into the console and Navigate to Amazon Keyspaces
2.	In the left hand pane select CQL editor
3.	Paste the contents of the file Keyspaces/Keyspace.sql in the editor and select Run command
4.	Next clear the contents of the editor and paste the contents of Keyspaces/Table_Schema.sql in the editor and select Run command
5.	Table creation can take upto a minute and you will be notified if the table is successfully created, and you can also view it by clicking Tables in the left hand pane

## Setup EMR
Setup an EMR cluster so we can run PySpark code to generate features. First, we need to setup a trust store password. Amazon Keyspaces requires the use of Transport Layer Security (TLS) to help secure connections with clients. To connect to Amazon Keyspaces using TLS, we need to download an Amazon digital certificate and configure the Python driver to use TLS. This certificate will be stored in a trust store and when retriving it we need to provide the correct password. In the file EMR/emr_bootstrap_script.sh update the line to a password you want to use:

```
# Create a JKS keystore from the certificate
PASS={your_truststore_password_here}
```

Next we need to update the app.config file to reflect the correct trust store password, in the file EMR/app.config update the value for truststore-password to the value you set above.
```
    }
    ssl-engine-factory {
      class = DefaultSslEngineFactory
      truststore-path = "/home/hadoop/.certs/cassandra_keystore.jks"
      truststore-password = "{your_password_here}"
    }
  }
```
Next we need to update the credentials for Keyspaces in the file EMR/app.config. Update the the following lines to reflect the username and password generated in the earlier step:

```
auth-provider {
    class = PlainTextAuthProvider
    username = "{your-keyspace-username}"
    password = "{your-keyspace-password}"
}
```

Next we create an EMR cluster. The code snippet below is an aws cli command which has Hadoop, Spark 3.0, Livy and JupyterHub installed. This also runs the bootstarping scipt on the cluster to set the connection to Keyspaces. You will need to update the path S3 bucket here the script is placed.

```
aws emr create-cluster --termination-protected --applications Name=Hadoop Name=Spark Name=Livy Name=Hive Name=JupyterHub --bootstrap-actions '[{"Path":"s3://{your-bucket-name-here}/EMR/emr_bootstrap_script.sh","Name":"Custom action"}]' --tags 'creator=feature-store-blogpost' --ebs-root-volume-size 10 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"{your-subnet-id}"}' --service-role EMR_DefaultRole --release-label emr-6.1.0 --log-uri 's3n://{your-bucket-name-here}/elasticmapreduce/' --name 'emr_feature_store' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core - 2"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region eu-west-1
```

Lastly we will create an EMR notebook instance to run the pyspark notebook Feature Creation and loading-notebook.ipynb (included in the repo). The following steps demonstrate this:
1.	In the console and navigate to EMR
2.	In the left hand pane select Notebooks
3.	Click Create Notebook at the top
4.	Give the notebook a name and select the cluster emr_feature_store which was created in the previous step
5.	Rest of the settings are optional. Select Create Notebook when you are done
6.	It can take a few minutes before the notebook instance is up and running. When it is in Ready status, select the notebook and choose either Open JupyterLab or Open Jupyter
7.	In the notebook instance import the notebook Feature Creation and loading-notebook.ipynb (included in the repo)
8.	Run the cells one by one to read the data from S3, create features and write these to Delta Lake and Keyspaces




### Simulation instructions

- For the simulation part, please refer to the folder Simulation

- In order to run the simulation, you will need to start a SageMaker notebook instance: https://docs.aws.amazon.com/sagemaker/latest/dg/nbi.html

- After you have launched your SageMaker notebook instance, start a new Jupyter notebook and choose the kernel conda_python3.
- Pull this repository with the command:
```sh
git pull <repository_url>
```
- Also you will need to put your certificate "sf-class2-root.crt" in the container folder for testing on the SageMaker instance.
For For your own use cases which might be sensitive and production environments consider changing to alternative and more secure solutions.
- Open the preconfigured Jupyter notebook: "ECS_Simulation.ipynb"
and follow the steps to set up the necessary resources to run the simulation. Especially make sure, to set the variables
necessary.


