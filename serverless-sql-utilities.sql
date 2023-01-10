SET QUOTED_IDENTIFIER OFF
GO
BEGIN TRY
EXEC('CREATE SCHEMA util');
END TRY
BEGIN CATCH END CATCH
GO
BEGIN TRY
EXEC('CREATE SCHEMA delta');
END TRY
BEGIN CATCH END CATCH
GO

-----------------------------------------------------------------------------------
--			Generic utilities
-----------------------------------------------------------------------------------
/*
DECLARE @data_source varchar(128) = ''
DECLARE @relative_path varchar(128) = ''
EXEC util.create_data_source
			'https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel',
			@data_source OUTPUT, 
			@relative_path OUTPUT 
PRINT @data_source
PRINT @relative_path
*/

CREATE OR ALTER PROCEDURE util.create_data_source
								@path varchar(1024),
								@credential varchar(1024) = null,
								@data_source varchar(128) OUTPUT,
								@relative_path varchar(128) OUTPUT
AS BEGIN
	DECLARE @tsql NVARCHAR(max);
	if(SUBSTRING(@path, 1, 8) NOT IN ('https://', 'abfss://'))
	begin
		raiserror('The @path must be absolute paths', 16, 1);
		return
	end
	SET @data_source = SUBSTRING(@path, 9, CHARINDEX('.',@path)-9);
	DECLARE @data_source_location varchar(1024) = SUBSTRING( @path, 0, CHARINDEX('/',@path, 10));
	SET @relative_path = SUBSTRING( @path, CHARINDEX('/',@path, 10), 1028);
	if(@credential is not null)
	BEGIN
		IF (@credential = 'Managed Identity') BEGIN
			CREATE DATABASE SCOPED CREDENTIAL [Managed Identity] WITH IDENTITY = 'Managed Identity';
			SET @credential = 'Managed Identity';
		END
		ELSE IF (SUBSTRING(@credential, 1, 4) = 'sas:') BEGIN
			set @tsql = CONCAT("CREATE DATABASE SCOPED CREDENTIAL [",@data_source,"]
									WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
									SECRET = '", SUBSTRING(@credential, 4, 1024), "'");
			PRINT 'Creating a database scoped credential...';
			PRINT @tsql;
			EXEC (@tsql);
			SET @credential = SUBSTRING(@credential, 4, 1024);
		END
			
	END
	IF(0 = (select count(*) from sys.external_data_sources where name = @data_source))
	begin
		SET @tsql = 
			CONCAT("CREATE EXTERNAL DATA SOURCE [", @data_source, "] WITH ( LOCATION = '",@data_source_location,"'",
				IIF(@credential IS NOT NULL, ", CREDENTIAL = " + @credential, ""),
			")");
	
		PRINT 'Creating external data source...'
		PRINT (@tsql)
		EXEC (@tsql) 
	end
END
GO

CREATE OR ALTER PROCEDURE util.create_file_format @file_format varchar(20)
AS BEGIN
	declare @tsql varchar(max) = null;
	if(@file_format IN ('DELTA', 'PARQUET'))
	begin
		SET @tsql = CONCAT("CREATE EXTERNAL FILE FORMAT [", @file_format, "] WITH ( FORMAT_TYPE = ",@file_format,")");
		EXEC(@tsql);	
	end else if (@file_format = 'CSV')
	begin
		SET @tsql = CONCAT("CREATE EXTERNAL FILE FORMAT [", @file_format, "]
									WITH ( FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS ( STRING_TERMINATOR = ',' ) )");
		EXEC(@tsql);
	end else if (@file_format = 'TSV')
	begin
		SET @tsql = CONCAT("CREATE EXTERNAL FILE FORMAT [", @file_format, "]
									WITH ( FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS ( STRING_TERMINATOR = '\t' ) )");
		EXEC(@tsql);
	end
END
GO

/*
EXEC util.create_table 'Test', 'https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel'
*/
CREATE OR ALTER PROCEDURE util.create_table @table_name sysname, @path varchar(1024),
						@file_format sysname, @data_source sysname = null, 
						@schema_name sysname = '', @database_name sysname = null
AS BEGIN

	DECLARE @tsql NVARCHAR(MAX);
	DECLARE @eds_location varchar(1024)
	DECLARE @table_location varchar(1024)

	IF(@data_source IS NULL AND SUBSTRING(@path, 1, 8) IN ('https://', 'abfss://'))
	-- Automaticaly create a data source based on the @path.
	BEGIN
		DECLARE @relative_path varchar(128) = '';
		EXEC util.create_data_source
					@path, NULL,
					@data_source OUTPUT, 
					@relative_path OUTPUT 
	END
	
	SELECT @eds_location = location FROM util.get_data_source_location(@data_source)

	IF(@eds_location IS NULL)
	BEGIN
		DECLARE @msg VARCHAR(8000);
		SET @msg = CONCAT('Cannot find external data source ', @data_source);
		RAISERROR (@msg, 16, 1)
		RETURN
	END

	IF(SUBSTRING(@path, 1, 8) IN ('https://', 'abfss://') AND PATINDEX('%'+@eds_location+'%', @path) = 0)
	BEGIN
		SET @msg = CONCAT('Path ', @path, ' is not referencing a folder within the external data source location: ', @eds_location);
		RAISERROR (@msg, 16, 1)
		RETURN
	END

	DECLARE @file_format_name varchar(20) = NULL;
	DECLARE @file_format_type varchar(20) = NULL;
	select @file_format_name = name, @file_format_type = format_type
	from sys.external_file_formats
	where name = @file_format or format_type = @file_format;

	if(@file_format_name IS NULL)
	begin
		exec util.create_file_format @file_format;
		SET @file_format_name = @file_format;
	end
	IF @file_format_name IS NULL BEGIN
		RAISERROR ( 'Cannot find external format type', 16, 1 );
		RETURN;
	END ELSE
	IF @file_format_type = 'CSV' BEGIN
		SET @file_format_type = "'CSV', PARSER_VERSION='2.0'";
	END ELSE
		SET @file_format_type = "'" + @file_format_type + "'";

	SET @tsql = CONCAT("SELECT TOP 0 * FROM OPENROWSET(BULK '", @path, "', FORMAT = ", @file_format_type, " ) as data");

	create table #frs (
		is_hidden bit not null,
		column_ordinal int not null,
		name sysname null,
		is_nullable bit not null,
		system_type_id int not null,
		system_type_name nvarchar(256) null,
		max_length smallint not null,
		precision tinyint not null,
		scale tinyint not null,
		collation_name sysname null,
		user_type_id int null,
		user_type_database sysname null,
		user_type_schema sysname null,
		user_type_name sysname null,
		assembly_qualified_type_name nvarchar(4000),
		xml_collection_id int null,
		xml_collection_database sysname null,
		xml_collection_schema sysname null,
		xml_collection_name sysname null,
		is_xml_document bit not null,
		is_case_sensitive bit not null,
		is_fixed_length_clr_type bit not null,
		source_server sysname null,
		source_database sysname null,
		source_schema sysname null,
		source_table sysname null,
		source_column sysname null,
		is_identity_column bit null,
		is_part_of_unique_key bit null,
		is_updateable bit null,
		is_computed_column bit null,
		is_sparse_column_set bit null,
		ordinal_in_order_by_list smallint null,
		order_by_list_length smallint null,
		order_by_is_descending smallint null,
		tds_type_id int not null,
		tds_length int not null,
		tds_collation_id int null,
		tds_collation_sort_id tinyint null
	);

	insert #frs
	exec sys.sp_describe_first_result_set @tsql;

	declare @column_schema nvarchar(max);
	set @column_schema = (select '(' + string_agg(QUOTENAME(name) + ' ' + system_type_name, ', ') + ')' from #frs);
	set @tsql = CONCAT("CREATE EXTERNAL TABLE ", 
						ISNULL(@database_name, DB_NAME()) + "." + @schema_name+".", @table_name, "
						",@column_schema, "
						WITH (	LOCATION = '"+ REPLACE( @path, @eds_location,'') +"', 
								DATA_SOURCE = [", @data_source, "],
								FILE_FORMAT = [", @file_format_name,"]);")
	PRINT 'Creating external table...'
	PRINT(@tsql)
	EXEC(@tsql)
END
GO

CREATE OR ALTER PROCEDURE util.die_table @table_name sysname, @schema_name sysname = ''
AS BEGIN
	IF(0<(SELECT count(*) FROM sys.external_tables 
							WHERE name = @table_name 
							AND SCHEMA_NAME(schema_id) = IIF(@schema_name = '', SCHEMA_NAME(), @schema_name)))
	BEGIN
		DECLARE @tsql NVARCHAR(4000) = 'DROP EXTERNAL TABLE ' + IIF(@schema_name = '', SCHEMA_NAME(), QUOTENAME(@schema_name)) + '.' + QUOTENAME(@table_name);
		PRINT(@tsql)
		EXEC(@tsql)
	END
END
GO

CREATE OR ALTER PROCEDURE util.create_or_alter_table @table_name sysname, @path varchar(1024),
						@file_format sysname, @data_source sysname = null, 
						@schema_name sysname = '', @database_name sysname = null
AS BEGIN
	EXEC util.die_table @table_name, @schema_name
	EXEC util.create_table @table_name, @path,
						@file_format, @data_source, 
						@schema_name, @database_name
END
GO


-----------------------------------------------------------------------------------
--			Delta Lake utilities
-----------------------------------------------------------------------------------

/*
SELECT * 
	FROM delta.get_table_location('timetravel')
*/
CREATE OR ALTER FUNCTION delta.get_table_location(@table varchar(128))
RETURNS TABLE
AS RETURN (
    select	delta_uri = TRIM('/' FROM eds.location) + '/' + TRIM('/' FROM et.location),
			delta_folder = et.location,
			data_source_location = eds.location,
			data_source_name = eds.name
	from sys.external_tables et 
    join sys.external_data_sources eds on et.data_source_id = eds.data_source_id
    join sys.external_file_formats ff on et.file_format_id = ff.file_format_id and LOWER(format_type) = 'delta'
    where et.name = @table
)
GO
/*
EXEC delta.describe_history 'https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel'
EXEC delta.describe_history 'timetravel'
*/
CREATE OR ALTER PROCEDURE delta.describe_history @name varchar(1024)
AS BEGIN
	DECLARE @location VARCHAR(1000);
	DECLARE @msg VARCHAR(8000);
	IF (SUBSTRING(@name, 1, 5) NOT IN ('https', 'abfss'))
	BEGIN
		SET @msg = CONCAT('Retrieving history for the table ', @name)
		PRINT (@msg)
		SELECT @location = delta_uri 
		FROM delta.get_table_location(@name)

		IF (@location IS NULL)
			RAISERROR('Cannot find an ADLS location for the table ''%s''', 16, 1, @name)
		ELSE
			EXEC delta.describe_history @location 
	END
	ELSE
	BEGIN
		SET @msg = CONCAT('Retrieving history for the Delta Lake location ', @name)
		PRINT (@msg)
		SET @location =TRIM('/' FROM @name)
		DECLARE @tsql VARCHAR(MAX);

		SET @tsql = CONCAT("
		SELECT version = CAST(result.filepath(1) AS BIGINT),
			timestamp = dateadd(s, CAST(JSON_VALUE (jsonContent, '$.commitInfo.timestamp') AS BIGINT)/1000, '19700101'),
			operation = JSON_VALUE (jsonContent, '$.commitInfo.operation')
		FROM
			OPENROWSET(
				BULK '", @location, "/_delta_log/*.json',
				FORMAT = 'CSV', FIELDQUOTE = '0x0b', FIELDTERMINATOR ='0x0b', ROWTERMINATOR = '0x0b' )
			WITH ( jsonContent varchar(MAX) ) AS [result]
		ORDER BY JSON_VALUE (jsonContent, '$.commitInfo.timestamp') DESC")
		EXEC(@tsql)
	END

END
GO

/*

EXEC delta.describe_history 'timetravel';

EXEC delta.snapshot	@name='https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel',
					@version = 23;
EXEC delta.snapshot	@name='timetravel',
					@version = 23;
EXEC delta.snapshot @name='https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel',
					@timestamp = '2022-09-19 18:57:00'
EXEC delta.snapshot @name='timetravel',
					@timestamp = '2022-09-19 18:57:00'
EXEC delta.snapshot @name='timetravel',
					@timestamp = '2022-09-19 18:56:00'

*/
SET QUOTED_IDENTIFIER OFF
GO
CREATE OR ALTER PROCEDURE delta.snapshot
				@name varchar(1024),
				@version int = null,
				@timestamp datetime2 = null,
				@view sysname = null,
				@schema sysname = 'dbo'
AS BEGIN

	IF(@version IS NULL AND @timestamp IS NULL) BEGIN
		RAISERROR('You need to specify @version or @timestamp parameters', 16, 1);			
		RETURN
	END

	IF(@version IS NOT NULL AND @timestamp IS NOT NULL) BEGIN
		RAISERROR('You cannot specify both @version and @timestamp parameters', 16, 1);			
		RETURN
	END
	SET QUOTED_IDENTIFIER OFF

	DECLARE @msg VARCHAR(1024);

	IF(@timestamp IS NOT NULL)
	BEGIN
		SET @msg = CONCAT('Searching for a version for the timestamp ', @timestamp)
		PRINT (@msg)

		DROP TABLE IF EXISTS #delta_history
		CREATE TABLE #delta_history([version] bigint, [timestamp] datetime2, operation varchar(max))

		INSERT INTO #delta_history
		EXEC delta.describe_history @name

		SELECT @version = cur.version
		FROM #delta_history cur JOIN #delta_history nex ON cur.version = nex.version - 1
		WHERE cur.timestamp <= @timestamp AND @timestamp < nex.timestamp

		SET @msg = CONCAT('Version for the timestamp ', @timestamp, ' is "', @version, '"')
		PRINT (@msg)
		SET @timestamp = NULL
		
	END
	
	DECLARE @location VARCHAR(1024);
	DECLARE @data_source SYSNAME = NULL;
	DECLARE @delta_folder SYSNAME = NULL;
	
	IF (SUBSTRING(@name, 1, 5) NOT IN ('https', 'abfss'))
	BEGIN
		SET @view = @name;
		SET @msg = CONCAT('Retrieving location for the table ', @name)
		PRINT (@msg)
		
		SELECT @location = delta_uri, @data_source = [data_source_name], @delta_folder = delta_folder
		FROM delta.get_table_location(@name)

		IF (@location IS NULL) BEGIN
			RAISERROR('Cannot find an ADLS location for the table ''%s''', 10, 1, @name);	
			RETURN
		END
		ELSE
		BEGIN
			SET @msg = CONCAT('Location for the table is "', @location, '"')
			PRINT (@msg)
		END
	END ELSE
	BEGIN
		DECLARE @invLocation VARCHAR(1024) = REVERSE(TRIM(@name));
		SET @view = SUBSTRING(@name, LEN(@name) - CHARINDEX('/', @invLocation) + 2, 128)
		SET @location = @name;
	END

	CREATE TABLE #log_files([version] BIGINT, [added_file] varchar(4000), [removed_file] varchar(4000), isCheckpoint BIT)
	INSERT INTO #log_files
	EXEC delta._get_latest_checkpoint_files @location, @version

	IF(0 = (SELECT COUNT(*) FROM #log_files)) BEGIN
		RAISERROR('Version "%i" is not available for time travel. Cannot find checkpoint before this version.', 19, 1, @version) WITH LOG;
		RETURN
	END
	ELSE
	BEGIN
		DECLARE @latest_checkpoint_version BIGINT
		select @latest_checkpoint_version = MAX(version) from #log_files

		INSERT INTO #log_files
		EXEC delta._get_latest_json_logs
			@location, @latest_checkpoint_version, @version;
			
		DECLARE @path_list VARCHAR(MAX) = '';
		DECLARE @path_count VARCHAR(MAX) = '';

		SELECT
			@path_count = COUNT(*), 
			@path_list = STRING_AGG(CAST(("'"+ISNULL(@delta_folder, @location)+'/'+added_file+"'") AS VARCHAR(MAX)),',')
		from #log_files a
			where added_file is not null
			and added_file not in (select removed_file from #log_files where removed_file is not null);

		--SELECT @path_count, @path_list

		DECLARE @tsql VARCHAR(MAX);
		SELECT @tsql = CONCAT(
	"CREATE OR ALTER VIEW "+ @schema +".[", @view, "@v", @version,"] 
	  AS SELECT * FROM OPENROWSET ( BULK ", 

					CASE
						WHEN @path_count = 1 THEN @path_list
						WHEN 1 < @path_count AND @path_count < 1024 THEN '('+@path_list+')'
						ELSE CONCAT('''', ISNULL(@delta_folder, @location) + '/**''')
					END
						, 
						IIF(@data_source IS NOT NULL, ", DATA_SOURCE = '" + @data_source + "'", ""),
						", FORMAT='PARQUET' ) as data"
						+ IIF (@path_count < 1024, "", 
								" WHERE data.filepath() IN " + '('+@path_list+')'))
		from #log_files a
			where added_file is not null
			and added_file not in (select removed_file from #log_files where removed_file is not null);

		PRINT(@tsql)
		EXEC(@tsql)
	
	END
END
GO

/*
EXEC delta._get_latest_checkpoint_files 
		'https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel/',
		20
*/
GO
CREATE OR ALTER PROCEDURE delta._get_latest_checkpoint_files 
			(@location varchar(1024), @asOfVersion int = 0)
AS BEGIN

    SET QUOTED_IDENTIFIER OFF

    DECLARE @tsql NVARCHAR(max) = CONCAT("
    with
    all_checkpoint_logs (version, added_file, removed_file) as (
    select version = CAST(a.filepath(1) AS BIGINT), added_file, removed_file
    from openrowset(bulk '", @location  ,"/_delta_log/*.checkpoint.parquet',
                    format='parquet')
            with ( [added_file] varchar(1024) '$.add.path', [removed_file] varchar(1024) '$.remove.path' ) as a
    where CAST(a.filepath(1) AS BIGINT) <= ", @asOfVersion, "
	and NOT ( [added_file] IS NULL AND [removed_file] IS NULL)
    ),
	last_checkpoint (version, added_file, removed_file) as (
		select version, added_file, removed_file from all_checkpoint_logs
		where version = (select max(version) from all_checkpoint_logs)
	)
	select *, isCheckpoint = 1 from last_checkpoint");

	EXEC(@tsql)
END
GO

/*
EXEC delta._get_latest_json_logs
		'https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel/',
		20, 22
*/
GO
CREATE OR ALTER PROCEDURE delta._get_latest_json_logs 
			(@location varchar(1024), @version bigint, @asOfVersion bigint = 0)
AS BEGIN

    -- Add remaining added/removed files from .json files after the last checkpoint

	DECLARE @tsql VARCHAR(MAX);
	SET @tsql = CONCAT("select version = CAST(r.filepath(1) AS BIGINT), added_file, removed_file, isCheckpoint = 0
    FROM 
        OPENROWSET(
            BULK '", @location, "/_delta_log/*.json',
            FORMAT='CSV', 
            FIELDTERMINATOR = '0x0b',
            FIELDQUOTE = '0x0b', 
            ROWTERMINATOR = '0x0A'
        )
        WITH (jsonContent NVARCHAR(max)) AS [r]
        CROSS APPLY OPENJSON(jsonContent)
            WITH (added_file nvarchar(1000) '$.add.path', removed_file nvarchar(1000) '$.remove.path') AS j
    WHERE (r.filepath(1) >", @version,") --> Take the changes after checkpoint
    AND ( r.filepath(1) <=", @asOfVersion," ) --> Take the changes before specified AS-OF version 
	and NOT ( [added_file] IS NULL AND [removed_file] IS NULL)
    ");
	EXEC(@tsql);
END

/*
EXEC delta.describe_history 'timetravel'
EXEC delta.snapshot 'timetravel', 22, @schema = 'delta'
select * FROM delta.[timetravel@v22]
select * from delta.get_table_location ('timetravel')
EXEC delta.snapshot
				@name='https://<storage account>.dfs.core.windows.net/my-delta-lake/time-travel',
				@version = 23, 
				@schema = 'delta';
SELECT * FROM delta.[time-travel@v23]
*/

GO
/*
EXEC delta.create_table	'TestTimeTravel', 
						'https://jovanpoptest.dfs.core.windows.net/my-delta-lake/time-travel'
*/
CREATE OR ALTER PROCEDURE delta.create_table @table_name sysname, @path varchar(1024),
						@data_source sysname = null, 
						@schema_name sysname = '', @database_name sysname = null
AS BEGIN
	EXEC util.create_table @table_name, @path, 'DELTA', @data_source, @schema_name, @database_name; 
END
GO

CREATE OR ALTER PROCEDURE delta.create_or_alter_table @table_name sysname, @path varchar(1024),
						@data_source sysname = null, 
						@schema_name sysname = '', @database_name sysname = null
AS BEGIN
	EXEC util.create_or_alter_table @table_name, @path, 'DELTA', @data_source, @schema_name, @database_name; 
END
GO


CREATE OR ALTER PROCEDURE delta._get_tables @path varchar(1024), @folder varchar(256) = '/*/*'
AS BEGIN

	DECLARE @tsql varchar(max);
	SET @tsql = CONCAT("
	SELECT	database_name = result.filepath(1),
			table_name = result.filepath(2),
			path = REPLACE(result.filepath(), '/_delta_log/_last_checkpoint', ''),
			version = CAST(JSON_VALUE(jsonContent, '$.version') AS BIGINT)
	FROM
		OPENROWSET(
			BULK '", TRIM('/' FROM @path), '/', TRIM('/' FROM @folder), "/_delta_log/_last_checkpoint',
			FORMAT = 'CSV',
			FIELDQUOTE = '0x0b',
			FIELDTERMINATOR ='0x0b',
			ROWTERMINATOR = '0x0b'
		)
		WITH ( jsonContent varchar(MAX) ) AS [result]");
	PRINT @tsql
	EXEC(@tsql);
END
GO

CREATE OR ALTER PROCEDURE delta.discover_tables @path varchar(1024)
AS BEGIN
	DROP TABLE IF EXISTS #delta_tables;
	CREATE TABLE #delta_tables (database_name sysname, table_name sysname, path varchar(1024), version bigint);
	INSERT INTO #delta_tables
	EXEC delta._get_tables @path;

	WITH a AS (
	SELECT *, sql = CONCAT("DROP TABLE IF EXISTS ", database_name, "..", table_name, ";") FROM #delta_tables
	union all
	SELECT *, sql = CONCAT("EXEC delta.create_table '",table_name, "' ,'", path, "', @database_name = '", database_name, "' ") FROM #delta_tables
	)
	SELECT sql FROM a
	ORDER BY database_name, table_name, sql

END
GO

CREATE OR ALTER FUNCTION util.get_data_source_location(@name varchar(128))
RETURNS TABLE
AS RETURN (
    select	eds.location
    from sys.external_data_sources eds
    where eds.name = @name
)
GO

CREATE OR ALTER PROCEDURE util.cetas
        @table_name sysname, 
        @location nvarchar(1024),
        @select nvarchar(max),
        @data_source sysname,
        @file_format sysname = 'PARQUET'
AS
BEGIN
    DECLARE @tsql NVARCHAR(max);

    SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal

    SET @tsql = CONCAT(
"CREATE EXTERNAL TABLE ", QUOTENAME(@table_name), "
 WITH ( 
     LOCATION = '", @location,"/", @table_name, "',
     DATA_SOURCE = ", @data_source, ",
     FILE_FORMAT = ", @file_format, "
)
AS", @select);

    PRINT (@tsql)
    EXEC (@tsql)
END
GO



/*
util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid;Key=s5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==',
			'Ecdc';

util.create_cosmosdb_view 
			'Account=synapselink-cosmosdb-sqlsample;Database=covid',
			'Ecdc',
			@key = 's5zarR2pT0JWH9k8roipnWxUYBegOuFGjJpSjGlR36y86cW0GQ6RaaG8kGjsRAQoWMw1QKTkkX8HQtFpJjC8Hg==';
*/
CREATE OR ALTER PROCEDURE util.create_cosmosdb_view (	@connection nvarchar(max),
														@container nvarchar(1000),
														@schema_name sysname = 'dbo',
														@key varchar(1024) = NULL,
														@credential sysname = NULL)
AS BEGIN

	DECLARE @tsql NVARCHAR(MAX) 

	IF(@key IS NOT NULL AND @credential IS NOT NULL)
	BEGIN
		PRINT 'Creating CosmosDB credential...'
		SET @tsql =
"DROP DATABASE SCOPED CREDENTIAL " + @credential + ";
CREATE DATABASE SCOPED CREDENTIAL " + @credential + "
		WITH	IDENTITY = 'SHARED ACCESS SIGNATURE', " + "
				SECRET = '" + @key + "';"
		PRINT (@tsql)
		EXEC (@tsql)
	END

	IF (@credential IS NULL) 
		SET @tsql = 
		"SELECT TOP 0 *
			FROM OPENROWSET( 
				'CosmosDB',
				'"+IIF(CHARINDEX('Key=',@connection)> 0, @connection, @connection+';Key='+@key)+"',
				"+QUOTENAME(@container) + " ) as data;"
	ELSE
		SET @tsql =
		"SELECT TOP 0 *
			FROM OPENROWSET( 
				PROVIDER = 'CosmosDB',
				CONNECTION = '"+@connection+"',
				OBJECT = '"+@container + "',
				CREDENTIAL = '"+@credential + "') as data;"

	create table #frs (
		is_hidden bit not null,
		column_ordinal int not null,
		name sysname null,
		is_nullable bit not null,
		system_type_id int not null,
		system_type_name nvarchar(256) null,
		max_length smallint not null,
		precision tinyint not null,
		scale tinyint not null,
		collation_name sysname null,
		user_type_id int null,
		user_type_database sysname null,
		user_type_schema sysname null,
		user_type_name sysname null,
		assembly_qualified_type_name nvarchar(4000),
		xml_collection_id int null,
		xml_collection_database sysname null,
		xml_collection_schema sysname null,
		xml_collection_name sysname null,
		is_xml_document bit not null,
		is_case_sensitive bit not null,
		is_fixed_length_clr_type bit not null,
		source_server sysname null,
		source_database sysname null,
		source_schema sysname null,
		source_table sysname null,
		source_column sysname null,
		is_identity_column bit null,
		is_part_of_unique_key bit null,
		is_updateable bit null,
		is_computed_column bit null,
		is_sparse_column_set bit null,
		ordinal_in_order_by_list smallint null,
		order_by_list_length smallint null,
		order_by_is_descending smallint null,
		tds_type_id int not null,
		tds_length int not null,
		tds_collation_id int null,
		tds_collation_sort_id tinyint null
	);

	insert #frs
	exec sys.sp_describe_first_result_set @tsql;

	declare @with_clause nvarchar(max);
	set @with_clause = (select ' WITH (' + string_agg(
												QUOTENAME(name) + ' ' +
												IIF( CHARINDEX("VARCHAR", system_type_name) = 0, system_type_name, system_type_name + ' COLLATE Latin1_General_100_BIN2_UTF8'),
											', ') + ')'
						from #frs);

	set @tsql = "CREATE OR ALTER VIEW " + QUOTENAME(@schema_name) + "." + QUOTENAME(@container) + " AS " + REPLACE(
					REPLACE(@tsql, "TOP 0", ""),
					") as data", ") " + @with_clause + ' as data');

	PRINT 'Creating CosmosDB view...'
	PRINT @tsql
	EXEC(@tsql)
END
GO

SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal
GO
-- Creates a disgnostic view on a folder where diagnostic settings are created.
-- Example usage: exec util.create_diagnostics 'https://jovanpoptest.dfs.core.windows.net/insights-logs-builtinsqlreqsended/'
CREATE OR ALTER PROCEDURE util.create_diagnostics @path varchar(1024)
AS BEGIN

	DECLARE @tsql VARCHAR(MAX);

	SET @tsql = CONCAT("DROP EXTERNAL DATA SOURCE [Diagnostics];
CREATE EXTERNAL DATA SOURCE [Diagnostics] WITH ( LOCATION = '", @path, "' );");

	EXEC(@tsql);

	SET @tsql = "CREATE OR ALTER VIEW util.diagnostics
AS SELECT
    subscriptionId = r.filepath(1),
    resourceGroup = r.filepath(2),
    workspace = r.filepath(3),
    year = CAST(r.filepath(4) AS SMALLINT),
    month = CAST(r.filepath(5) AS TINYINT),
    day = CAST(r.filepath(6) AS TINYINT),
    hour = CAST(r.filepath(7) AS TINYINT),
    minute = CAST(r.filepath(8) AS TINYINT),
    details.queryType,
    durationS = CAST(details.durationMs / 1000. AS NUMERIC(8,1)),
    dataProcessedMB = CAST(details.dataProcessedBytes /1024./1024 AS NUMERIC(16,1)),
    details.distributedStatementId,
    details.queryText,
    details.startTime,
    details.endTime,
    details.resultType,
    --details.queryHash,
    details.operationName,
    details.endpoint,
    details.resourceId,
    details.error
FROM
    OPENROWSET(
        BULK 'resourceId=/SUBSCRIPTIONS/*/RESOURCEGROUPS/*/PROVIDERS/MICROSOFT.SYNAPSE/WORKSPACES/*/y=*/m=*/d=*/h=*/m=*/*.json',
        DATA_SOURCE = 'Diagnostics',
        FORMAT = 'CSV',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b'
    )
    WITH (
        jsonContent varchar(MAX)
    ) AS r CROSS APPLY OPENJSON(jsonContent)
                        WITH (  endpoint varchar(128) '$.LogicalServerName',
                                resourceGroup varchar(128) '$.ResourceGroup',
                                startTime datetime2 '$.properties.startTime',
                                endTime datetime2 '$.properties.endTime',
                                dataProcessedBytes bigint '$.properties.dataProcessedBytes',
                                durationMs bigint,
                                loginName varchar(128) '$.identity.loginName',
                                distributedStatementId varchar(128) '$.properties.distributedStatementId',
                                resultType varchar(128) ,
                                queryText varchar(max) '$.properties.queryText',
                                queryHash varchar(128) '$.properties.queryHash',
                                operationName varchar(128),
				error varchar(128) '$.properties.error',
                                queryType varchar(128) '$.properties.command',
				resourceId varchar(1024) '$.resourceId'
                             ) as details";

		EXEC(@tsql);
END
GO
SET QUOTED_IDENTIFIER OFF
GO
CREATE OR ALTER PROCEDURE util.list_files @uri_pattern varchar(8000), @template varchar(max) = '', @_delta_log_last_checkpoint bit = 0
AS BEGIN

	DECLARE @tsql NVARCHAR(MAX);
	SET @tsql = "
with parts as (
select	
		uri = " +
		CASE @_delta_log_last_checkpoint WHEN 0 THEN " f.filepath() "
			ELSE " REPLACE(f.filepath(),'/_delta_log/_last_checkpoint', '') "
		END
		+",
		domain = substring( f.filepath(),
							patindex('%://%',f.filepath())+3,
							charindex(	'/',
										f.filepath(), 
										patindex('%://%',f.filepath())+3) - (patindex('%://%',f.filepath())+3)
							),
		container = substring(	f.filepath(),
								charindex('/', f.filepath(),  patindex('%://%',f.filepath())+3)+1,
				/*cont_end=*/(charindex('/', f.filepath(),  charindex('/', f.filepath(),  patindex('%.net/%',f.filepath())+5))-1)
								-
				/*cont_start=*/(charindex('/', f.filepath(),  patindex('%://%',f.filepath())+3))
							),
		prefix = substring(	f.filepath(),
				/*cont_end=*/(charindex('/', f.filepath(),  charindex('/', f.filepath(),  patindex('%.net/%',f.filepath())+5))),
				patindex('%'+f.filepath(1)+'%',f.filepath())
				-
				/*cont_end=*/(charindex('/', f.filepath(),  charindex('/', f.filepath(),  patindex('%.net/%',f.filepath())+5)))
				),
		folder = f.filepath(1),
		suffix = " +
		CASE @_delta_log_last_checkpoint WHEN 0 
				THEN " substring(f.filepath(), patindex('%'+f.filepath(1)+'%',f.filepath())+len(f.filepath(1)),8000) "
				ELSE " REPLACE(substring(f.filepath(), patindex('%'+f.filepath(1)+'%',f.filepath())+len(f.filepath(1)),8000),'/_delta_log/_last_checkpoint', '') "
		END + " 
from openrowset(bulk '"+@uri_pattern+"',
					format='csv',
					fieldterminator ='0x0b',
					fieldquote = '0x0b')
with(a varchar(max)) as f
),
abfss_cte as (
select abfss = concat('abfss://',container,'@',domain,prefix,folder,suffix), parts.*
from parts
)
SELECT *" + 
	CASE @template WHEN '' THEN ''
	ELSE ", script = REPLACE('"+@template+"', '{abfss}',abfss)"
	--ELSE "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE('"+@template+"', '{domain}',domain), '{container}',container), '{prefix}',prefix), '{folder}',folder), '{suffix}',suffix), '{abfss}',abfss)"
	END +
" FROM abfss_cte";
	EXEC(@tsql)

END
GO
CREATE OR ALTER PROCEDURE delta.list_folders @uri_pattern varchar(8000), @template varchar(max) = '', @_delta_log_last_checkpoint bit = 0
AS BEGIN
	SET @uri_pattern = @uri_pattern + '/_delta_log/_last_checkpoint';
	EXEC util.list_files @uri_pattern, @template, 1
END
