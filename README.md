# Synapse Sql Utilities

Utility scripts that can help you to build the solutions using SQL pools in Synapse Analytics workspace.

# Setup

Download the file [Serverless SQL utilities](serverless-sql-utilities.sql) and execute the T-SQL script on your serverless SQL pool. Do not use `master` database.

# Usage

## CosmosDB

Synapse SQL utilities enable you create views on top of CosmosDB container:

```sql
util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid;Key=s5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==',
			'Ecdc';

util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid',
			'Ecdc',
			@key = 's5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==';
```

### Delta Lake

Use the procedure `delta.create_table` to create a table on data set placed on Data Lake:

```sql
delta.create_table 'TimeTravel', 'abfss://<container>@<storage account>.dfs.core.windows.net/time-travel'

select top 10 * from TimeTravel
```

Use the procedure `delta.describe_history` to see the history of changes in Delta Lake:

```sql
delta.describe_history 'TimeTravel'
```

Use the procedure `delta.snapshot` to create a view that represents a snapshot of the Delta Lake table at the specified version. Provide a table name
and specify the version, and you will get a view in the format `<table name>@v<version number>`:

```sql
delta.snapshot 'TimeTravel', 21

select * from TimeTravel@v21

delta.snapshot 'TimeTravel', 17

select * from TimeTravel@v17
```
