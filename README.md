# Synapse Sql Utilities

Utility scripts that can help you to build the solutions using SQL pools in Synapse Analytics workspace.

# Setup

Download the file [Serverless SQL utilities](serverless-sql-utilities.sql) and execute the T-SQL script on your serverless SQL pool. Do not use `master` database.

# Usage

## CosmosDB

Synapse SQL utilities enable you create views on top of CosmosDB container. 

You need to specify the CosmosDB connection string, container name, and the database scoped credential that contains CosmosDB account key.

```sql
util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid',
			'Ecdc',
			@credential = 'CosmosDBSampleCredential';
```

This script will create a `dbo.Ecdc` view that can read data from the CosmosDB analytical storage.

The procedure can automatically create a database scoped credential if you specify the ComsosDB key:

```sql
util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid',
			'Ecdc',
			@credential = 'CosmosDBSampleCredential',  --> This will drop previous credential to create a new one
			@key = 's5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==';
```

NOTE: This call will delete previous credential and create a new one!

The third option is to create CosmosDB view with the inline key that is placed in the view definition:

```sql
util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid;Key=s5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==',
			'Ecdc';

util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid',
			'Ecdc',
			@key = 's5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==';
```

This might not be good option from the security perspective because someone might see the key in the view definition and you would need to regenerate the views
if the CosmosDB key is re-generated.

### Delta Lake

Use the procedure `delta.create_table` to create a table on data set placed on Data Lake:

```sql
delta.create_table 'TimeTravel', 'abfss://<container>@<storage account>.dfs.core.windows.net/time-travel'

SELECT TOP 10 * FROM TimeTravel
```

Use the procedure `delta.describe_history` to see the history of changes in Delta Lake:

```sql
delta.describe_history 'TimeTravel'
```

Use the procedure `delta.snapshot` to create a view that represents a snapshot of the Delta Lake table at the specified version. Provide a table name
and specify the version, and you will get a view in the format `<table name>@v<version number>`:

```sql
delta.snapshot 'TimeTravel', 21

SELECT TOP 10 * FROM TimeTravel@v21;

delta.snapshot 'TimeTravel', 17

SELECT TOP 10 * FROM TimeTravel@v17;

delta.snapshot 'TimeTravel', @timestamp = '2022-09-23 11:48:19.000'

-- Assumption is that v66 is created for the timestamp '2022-09-23 11:48:19.000'
SELECT TOP 10 * FROM TimeTravel@v66;

```
