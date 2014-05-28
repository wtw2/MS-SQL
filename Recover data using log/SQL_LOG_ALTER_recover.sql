/******
(c) Walk the Way, 2014 (see accompanying license)
To use this:
1) SQL Server Management Studio must be "Run as administrator" (or using an admin account)
2) The SQL login User must have SQL sysadmin Server Role
For running DBCC PAGE: it appears that the SQL User must have SQL sysadmin Server Role
For sys.fn_dblog: the SQL User must have SELECT permission for "master" in System Databases
For SQL 2008 uncomment line 1150 (DATE not suppported in SQL 2005)
******/


IF EXISTS (SELECT *
           FROM   sys.objects
           WHERE  object_id = OBJECT_ID(N'[dbo].[Recover_Modified_Data_Proc]')
                  AND TYPE IN ( N'FN', N'IF', N'TF', N'FS', N'FT', N'P' ))
	SET NOEXEC ON

--> Based on the work of Muhammad Imran generously published at:
--> http://raresql.com/2012/02/01/how-to-recover-modified-records-from-sql-server-part-1/
--> 
--> Uncomment/Comment block below for testing/development
/****************************************
SET NOEXEC OFF
IF OBJECT_ID (N'[@ResultTable]', N'U') IS NOT NULL DROP TABLE [@ResultTable]
IF OBJECT_ID('tempdb..#T1') IS NOT NULL DROP TABLE #T1
IF OBJECT_ID('tempdb..#temp_Data') IS NOT NULL DROP TABLE #temp_Data
IF OBJECT_ID('tempdb..#CTE') IS NOT NULL DROP TABLE #CTE
IF OBJECT_ID('tempdb..#TransIdUpdateList') IS NOT NULL DROP TABLE #TransIdUpdateList
IF OBJECT_ID('tempdb..#TransIdDeleteList') IS NOT NULL DROP TABLE #TransIdDeleteList
IF OBJECT_ID('tempdb..#TransIdAllList') IS NOT NULL DROP TABLE #TransIdAllList
IF OBJECT_ID('tempdb..#AllocIdList') IS NOT NULL DROP TABLE #AllocIdList
IF OBJECT_ID('tempdb..#SysFnDblog') IS NOT NULL DROP TABLE #SysFnDblog
IF OBJECT_ID('tempdb..#DelRows') IS NOT NULL DROP TABLE #DelRows
DECLARE @Database_Name NVARCHAR(MAX)
DECLARE @SchemaName_n_TableName NVARCHAR(MAX)
DECLARE @Date_From DATETIME
DECLARE @Date_To DATETIME
SET @Database_Name = 'Test'
SET @SchemaName_n_TableName = 'Student'
SET @Date_From = '2014/05/22' --'1900/01/01'
SET @Date_To   = '2099/01/01' --'9999/12/31'
print @Date_From
print @Date_To
****************************************/
--> Uncomment/Comment block below to create Stored Procedure
/****************************************/
IF OBJECT_ID('Recover_Modified_Data_Proc', 'P') IS NOT NULL
	DROP PROC Recover_Modified_Data_Proc
GO
Create PROCEDURE Recover_Modified_Data_Proc
@Database_Name NVARCHAR(MAX),
@SchemaName_n_TableName NVARCHAR(MAX),
@Date_From DATETIME = '1900/01/01', --> Null equivalent
@Date_To   DATETIME = '9999/12/31'   --> largest date
AS

--> For SQL 2005; PIVOT requires compatibility_level = 90
IF ((SELECT [compatibility_level] from sys.databases where [name] = @Database_Name) < 90) BEGIN
	RETURN -1
END
/****************************************/

DECLARE @Debug INT
SET @Debug = 0
DECLARE @parms NVARCHAR(1024)
DECLARE @Fileid INT
DECLARE @hex_pageid AS VARCHAR(MAX)
DECLARE @Pageid INT
DECLARE @Slotid INT
DECLARE @TransactionName VARCHAR(MAX)
DECLARE @CurrentLSN VARCHAR(MAX)
DECLARE @BeginTime DATETIME
DECLARE @RowLogContents0 VARBINARY(8000)
DECLARE @RowLogContents1 VARBINARY(8000)
DECLARE @RowLogContents2 VARBINARY(8000)
DECLARE @RowLogContents3 VARBINARY(8000)
DECLARE @RowLogContents3_Var VARCHAR(MAX)
 
DECLARE @RowLogContents4 VARBINARY(8000)
DECLARE @LogRecord VARBINARY(8000)
DECLARE @LogRecord_Var VARCHAR(MAX)
 
DECLARE @ConsolidatedPageID VARCHAR(MAX)
DECLARE @AllocUnitID BIGINT
DECLARE @TransactionID VARCHAR(MAX)
DECLARE @Operation VARCHAR(MAX)
DECLARE @DatabaseCollation VARCHAR(MAX)

--FOR @Operation ='LOP_MODIFY_COLUMNS'                   
DECLARE @RowLogData_Var VARCHAR(MAX)
DECLARE @RowLogData_Hex VARBINARY(MAX)

DECLARE @TotalFixedLengthData INT 
DECLARE @FixedLength_Offset INT
DECLARE @VariableLength_Offset INT
DECLARE @VariableLength_Offset_Start INT
DECLARE @VariableLengthIncrease INT
DECLARE @FixedLengthIncrease INT
DECLARE @OldFixedLengthStartPosition INT
DECLARE @FixedLength_Loc INT
DECLARE @VariableLength_Loc INT
DECLARE @FixedOldValues VARBINARY(MAX)
DECLARE @FixedNewValues VARBINARY(MAX)
DECLARE @VariableOldValues VARBINARY(MAX)
DECLARE @VariableNewValues VARBINARY(MAX)


 
/*  Pick The actual data
*/
DECLARE @temppagedata TABLE(
	 [RecordID]     INT IDENTITY(1,1)
	,[ParentObject] sysname
	,[Object]       sysname
	,[Field]        sysname
	,[Value]        sysname
	)
 
DECLARE @pagedata TABLE(
	 [Page Index]   int
	,[DBCCid]       int
	,[ParentObject] sysname
	,[Object]       sysname
	,[Field]        sysname
	,[Value]        sysname
	)

DECLARE @pageindex TABLE(
	 [PageIndexID]        INT IDENTITY(1,1)
	,[ConsolidatedPageID] VARCHAR(MAX)
	,[Fileid]             VARCHAR(MAX)
	,[hex_pageid]         VARCHAR(MAX)
	,[Pageid]             VARCHAR(MAX)
	)

IF (@Debug > 2) BEGIN
	--select * from sys.fn_dblog(NULL, NULL)
	--> http://db4breakfast.blogspot.com/2013/03/fndblog-function-documentation.html
	--> IMPORTANT: [databasename] > Properties > Options > Recovery Model:  Full
	--> http://msdn.microsoft.com/en-us/library/ms189275.aspx --> Recovery Models (SQL Server)
	--> http://technet.microsoft.com/en-us/library/ms189272.aspx
	--> ALTER DATABASE model SET RECOVERY FULL
	--> Immediately after switching from the simple recovery model to the full recovery model or bulk-logged recovery model, take a full or differential database backup to start the log chain.
	--> The switch to the full or bulk-logged recovery model takes effect only after the first data backup.
	SELECT name, recovery_model_desc
	   FROM sys.databases
		  WHERE name = 'model' ;

	select 'Log file usage'
	SELECT name AS [File Name], 
			physical_name AS [Physical Name], 
			size/128.0 AS [Total Size in MB], 
			size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0 AS [Available Space In MB], 
			[growth], [file_id]
			,B.cntr_value as "LogFullPct"
			FROM sys.database_files A
				INNER JOIN sys.dm_os_performance_counters B ON RTRIM(B.instance_name)+'_log' = A.name
			WHERE type_desc = 'LOG'
				AND B.counter_name LIKE 'Percent Log Used%'
				AND B.instance_name not in ('_Total', 'mssqlsystemresource')
	--SELECT instance_name as [Database],cntr_value as "LogFullPct"
	--	FROM sys.dm_os_performance_counters A
	--	WHERE counter_name LIKE 'Percent Log Used%'
	--		AND instance_name not in ('_Total', 'mssqlsystemresource')
	--		AND cntr_value > 0;
END
IF (@Debug > 3) BEGIN
	select 'sys.fn_dblog(NULL, NULL)'
	--> LSN = Log Sequence Number
	select [Current LSN],[Operation],[Context],[TRANSACTION ID],[Transaction Name],[Previous LSN],[AllocUnitId],[AllocUnitName],[Begin Time]
		,[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],[RowLog Contents 3],[RowLog Contents 4],SUSER_SNAME([Transaction SID]) AS [Transaction Account]
	from sys.fn_dblog(NULL, NULL)
	where [TRANSACTION ID] IN (SELECT DISTINCT [TRANSACTION ID] FROM sys.fn_dblog(NULL, NULL) WHERE [Transaction Name] IN ('UPDATE','DELETE','INSERT'))
	order by [Current LSN]
	--select * from sys.allocation_units
END

--> Get [TRANSACTION ID]s & [Transaction Name]s for INSERTs, DELETEs, UPDATEs in the time range
-- This includes: [Operation]s of ('LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS','LOP_INSERT_ROWS','LOP_DELETE_ROWS')
	/*Use this subquery to filter the date*/
SELECT DISTINCT [TRANSACTION ID],[Transaction Name],SUSER_SNAME([Transaction SID]) AS [Transaction Account]
	INTO #TransIdAllList
	FROM sys.fn_dblog(NULL, NULL)
	WHERE [Context] IN ('LCX_NULL') AND [Operation] IN ('LOP_BEGIN_XACT')  
		AND [Transaction Name] IN ('UPDATE','INSERT','DELETE')
		AND CONVERT(NVARCHAR(11),[Begin Time]) BETWEEN @Date_From AND @Date_To

--> Now remove all ('UPDATE','INSERT','DELETE')_transactions that are NOT ('LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS','LOP_INSERT_ROWS','LOP_DELETE_ROWS')_transactions
DELETE FROM #TransIdAllList WHERE [TRANSACTION ID] IN
(SELECT DISTINCT [TRANSACTION ID]
	--INTO #TransIdAllList
	FROM sys.fn_dblog(NULL, NULL)
	WHERE [TRANSACTION ID] IN (SELECT [TRANSACTION ID] FROM #TransIdAllList)
		AND [Operation] NOT IN (NULL,'LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS','LOP_INSERT_ROWS','LOP_DELETE_ROWS'))

--> Get [TRANSACTION ID]s for UPDATEs in time range; both 'LOP_MODIFY_ROW' & 'LOP_MODIFY_COLUMNS'
SELECT [TRANSACTION ID],[Transaction Name]
	INTO #TransIdUpdateList
	FROM #TransIdAllList
	WHERE [Transaction Name] IN ('UPDATE')

--> Get [TRANSACTION ID]s for DELETEs in time range
-- WARNING: the @Date_To needs to include the DELETE for deleted UPDATEs
SELECT DISTINCT [TRANSACTION ID]
	INTO #TransIdDeleteList
	FROM #TransIdAllList
	WHERE [Transaction Name] IN ('DELETE')

--> Get appropriate [Allocation_unit_id]s
SELECT [Allocation_unit_id]
	INTO #AllocIdList
	FROM sys.allocation_units allocunits
			INNER JOIN sys.partitions partitions ON
				(allocunits.type IN (1, 3) AND partitions.hobt_id = allocunits.container_id)
				OR (allocunits.type = 2 AND partitions.partition_id = allocunits.container_id)  
	WHERE object_id=object_ID('' + @SchemaName_n_TableName + '')

--> Get the sys.fn_dblog record info for all transactions of concern
SELECT [Current LSN],[Operation],[Context],[PAGE ID],[AllocUnitId],[Slot ID],[TRANSACTION ID],[Transaction Name],[Begin Time],[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],[RowLog Contents 3],[RowLog Contents 4],[Log Record Length],[Log Record Fixed Length],[Log Record]
	INTO #SysFnDblog
	FROM sys.fn_dblog(NULL, NULL)
	WHERE [TRANSACTION ID] IN (SELECT [TRANSACTION ID] FROM #TransIdAllList)
--select * from #SysFnDblog

--> Get record info for deleted rows for UPDATEs that may have been deleted
SELECT [PAGE ID],[AllocUnitId],[Slot ID],[TRANSACTION ID],MAX([RowLog Contents 0]) AS [RowLog Contents 0] --> removes NULLs
	INTO #DelRows
	FROM #SysFnDblog
	WHERE [Operation] IN ('LOP_DELETE_ROWS')
		AND [AllocUnitId] IN (SELECT * FROM #AllocIdList)
		AND [TRANSACTION ID] IN (SELECT * FROM #TransIdDeleteList)
	GROUP BY [PAGE ID],[AllocUnitId],[Slot ID],[TRANSACTION ID]

IF (@Debug > 1) BEGIN
	SELECT '#TransIdAllList'
	SELECT * FROM #TransIdAllList
	SELECT '#TransIdUpdateList'
	SELECT * FROM #TransIdUpdateList
	SELECT '#TransIdDeleteList'
	SELECT * FROM #TransIdDeleteList
	SELECT '#AllocIdList'
	SELECT * FROM #AllocIdList
	SELECT '#SysFnDblog '
	SELECT * FROM #SysFnDblog 
	print 'select * from #DelRows'
	SELECT '#DelRows' 
	SELECT * FROM #DelRows 
END

DECLARE @ModifiedRawData TABLE(
	 [ID]                    INT IDENTITY(1,1)
	,[PAGE ID]               VARCHAR(MAX)
	,[Slot ID]               INT
	,[AllocUnitId]           BIGINT
	,[Datum]                 VARCHAR(20)
	,[TRANSACTION ID]        VARCHAR(MAX)
	,[Transaction Name]      VARCHAR(MAX)
	,[Current LSN]           VARCHAR(MAX)
	,[EntryID]               INT DEFAULT -1
	,[PrevEntry]             INT DEFAULT -1
	,[NextEntry]             INT DEFAULT -1
	,[ChainLevel]            INT DEFAULT -1
	,[Process]               INT DEFAULT -1
	,[Begin Time]            DATETIME
	,[RowLog Contents 0_var] VARCHAR(MAX)
	,[RowLog Contents 0]     VARBINARY(8000)
	,[Slot Info]             VARCHAR(MAX)
	)

--> Get UPDATEd records information
INSERT INTO @ModifiedRawData ([PAGE ID],[Slot ID],[AllocUnitId],[Datum],[TRANSACTION ID],[Transaction Name],[Current LSN],[Begin Time],[RowLog Contents 0_var])
	SELECT A.[PAGE ID],A.[Slot ID],A.[AllocUnitId],'Reference' AS [Datum],A.[TRANSACTION ID]
		,(SELECT [Transaction Name] FROM #TransIdUpdateList WHERE [TRANSACTION ID] = A.[TRANSACTION ID]) AS [Transaction Name] --> always = 'UPDATE' here
		,A.[Current LSN]
		,(SELECT MAX([Begin Time]) FROM #SysFnDblog WHERE [TRANSACTION ID] = A.[TRANSACTION ID]) AS [Begin Time] --> eliminates NULL values
		,NULL AS [Value] --> [RowLog Contents 0_var]
	FROM #SysFnDblog A
	WHERE A.[AllocUnitId] IN (SELECT * FROM #AllocIdList)
		AND [Operation] IN ('LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS')
		AND [Context] IN ('LCX_HEAP','LCX_CLUSTERED')
		AND [TRANSACTION ID] IN (SELECT [TRANSACTION ID] FROM #TransIdUpdateList)
	GROUP BY A.[PAGE ID],A.[Slot ID],A.[AllocUnitId],[Begin Time],[Transaction ID],[Transaction Name],A.[Current LSN]
	ORDER BY [Slot ID],[Current LSN] --> [Slot ID] is the record entry number
--SELECT * FROM @ModifiedRawData 

--> Get the indexing information for DBCC PAGE
INSERT INTO @pageindex ([ConsolidatedPageID],[Fileid],[hex_pageid],[Pageid])
	SELECT DISTINCT [PAGE ID],NULL,NULL,NULL FROM @ModifiedRawData
UPDATE @pageindex
	SET [Fileid] = SUBSTRING([ConsolidatedPageID],0,CHARINDEX(':',[ConsolidatedPageID])) -- Seperate File ID from Page ID
	, [hex_pageid] = '0x'+ SUBSTRING([ConsolidatedPageID],CHARINDEX(':',[ConsolidatedPageID])+1,LEN([ConsolidatedPageID]))  -- Seperate the page ID
UPDATE @pageindex
	SET [Pageid] = CONVERT(INT,CAST('' AS XML).value('xs:hexBinary(substring(sql:column("[hex_pageid]"),sql:column("t.pos")) )', 'varbinary(max)')) -- Convert Page ID from hex to integer
		FROM (SELECT CASE SUBSTRING([hex_pageid], 1, 2) WHEN '0x' THEN 3 ELSE 0 END FROM @pageindex A WHERE A.[PageIndexID] = [PageIndexID]) AS t(pos)
--SELECT * FROM @pageindex

/*******************CURSOR START*********************/
--> Get DBCC PAGE data for each [ConsolidatedPageID]
DECLARE Page_Data_Cursor CURSOR FOR
	SELECT 	[Fileid],[Pageid]
		FROM @pageindex
	ORDER BY [Fileid],[Pageid]
/****************************************/
	OPEN Page_Data_Cursor

    FETCH NEXT FROM Page_Data_Cursor INTO @Fileid,@Pageid
    WHILE @@FETCH_STATUS = 0
	BEGIN
		DELETE @temppagedata
		 --> the next line requires cursor use due to augument requirements
		INSERT INTO @temppagedata EXEC( 'DBCC PAGE(' + @DataBase_Name + ', ' + @Fileid + ', ' + @Pageid + ', 3) with tableresults,no_infomsgs;')
		--> Concatenante all DBCC PAGE pages into one file by [ConsolidatedPageID] using index [PageIndexID]
		INSERT INTO @pagedata ([Page Index],[DBCCid],[ParentObject],[Object],[Field],[Value])
			SELECT B.[PageIndexID],A.[RecordID],[ParentObject],[Object],[Field],[Value]
			FROM @temppagedata A
				JOIN @pageindex B ON B.[Pageid] = @Pageid AND B.[Fileid] = @Fileid
		FETCH NEXT FROM Page_Data_Cursor INTO @Fileid,@Pageid
	END
 
CLOSE Page_Data_Cursor
DEALLOCATE Page_Data_Cursor
/*******************CURSOR END*********************/

--> Add the DBCC PAGE information OR DELETEd info to the UPDATEd records information
-- DBCC PAGE indexing is independent of [AllocUnitId]
UPDATE @ModifiedRawData
	SET
		[RowLog Contents 0_var] = 
			COALESCE( --> look for DBCC PAGE record set
				UPPER((SELECT REPLACE(STUFF(
					(SELECT REPLACE(SUBSTRING([VALUE],CHARINDEX(':',[Value])+1,48),'†','')
					FROM @pagedata C
					WHERE B.[Page Index] = C.[Page Index] --> EQUIVALENT to [ConsolidatedPageID]
						AND A.[Slot ID] = LTRIM(RTRIM(SUBSTRING(C.[ParentObject],5,3)))
						AND [Object] LIKE '%Memory Dump%'
					GROUP BY [Value] FOR XML PATH('') 
					),1,1,'') ,' ','')
				)) --> next, look for deleted record
				,UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("C.[RowLog Contents 0]") )', 'varchar(max)'))
				,'ERROR: '+B.[ParentObject] --> Missing [Slot ID] in "DBCC PAGE", and no corresponding DELETE found
			)
		,[Slot Info] = COALESCE(B.[ParentObject],'Slot '+CONVERT(VARCHAR,C.[Slot ID])+' deleted')
	FROM @ModifiedRawData A
		INNER JOIN @pageindex D ON D.[ConsolidatedPageID] = A.[PAGE ID]
		LEFT JOIN  @pagedata B ON
			B.[Page Index] = D.[PageIndexID]
			AND A.[Slot ID] = LTRIM(RTRIM(SUBSTRING(B.[ParentObject],5,3)))
			AND B.[Object] LIKE '%Memory Dump%'
		LEFT JOIN #DelRows C ON
			C.[PAGE ID]=[A].[PAGE ID]
			AND C.[AllocUnitId] = A.[AllocUnitId]
			AND C.[Slot ID] = A.[Slot ID]
		
--> Convert the old data which is in string format to hex format (required).
UPDATE @ModifiedRawData SET [RowLog Contents 0] = CAST('' AS XML).value('xs:hexBinary(substring(sql:column("[RowLog Contents 0_var]"), 0) )', 'varbinary(max)')
	FROM @ModifiedRawData 

IF (@Debug > 0) BEGIN
	select '@pageindex'
	select * from @pageindex
	select '@DBCC PAGE'
	select * from @temppagedata
	select '@pagedata'
	select * from @pagedata --ORDER BY [Page ID],[AllocUnitId],[Current LSN],[DBCCid]
	print 'select * from @ModifiedRawData1'
	select '@ModifiedRawData1'
	select * from @ModifiedRawData
END

DECLARE @PreliminaryRawData TABLE(
	 [ID]                    INT IDENTITY(1,1)
	,[PAGE ID]               VARCHAR(MAX)
	,[Slot ID]               INT
	,[AllocUnitId]           BIGINT
	,[Datum]                 VARCHAR(20)
	,[TRANSACTION ID]        VARCHAR(MAX)
	,[Transaction Name]      VARCHAR(MAX)
	,[Current LSN]           VARCHAR(MAX)
	,[Begin Time]            DATETIME
	,[RowLog Contents 0]     VARBINARY(8000)
	,[RowLog Contents 1]     VARBINARY(8000)
	,[RowLog Contents 2]     VARBINARY(8000)
	,[RowLog Contents 3]     VARBINARY(8000)
	,[RowLog Contents 4]     VARBINARY(8000)
	,[Log Record]            VARBINARY(8000)
	,[Operation]             VARCHAR(MAX)
	,[RowLog Contents 0_var] VARCHAR(MAX)
	,[RowLog Contents 1_var] VARCHAR(MAX)
	)

---Now we have the modifed data plus its slot ID , page ID and allocunit as well.
--After that we need to get the old values before modfication, these data are in chunks.
INSERT INTO @PreliminaryRawData
	SELECT [PAGE ID],[Slot ID],[AllocUnitId],'Restoration' AS [Datum],[TRANSACTION ID]
		,(SELECT [Transaction Name] FROM #TransIdAllList WHERE [TRANSACTION ID] = A.[TRANSACTION ID]) AS [Transaction Name]
		,[Current LSN]
		,(SELECT MAX([Begin Time]) FROM #SysFnDblog WHERE [TRANSACTION ID] = A.[TRANSACTION ID]) AS [Begin Time] --> eliminates NULL values
		,[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],[RowLog Contents 3],[RowLog Contents 4]
		,SUBSTRING ([Log Record],[Log Record Fixed Length],([Log Record Length]+1)-([Log Record Fixed Length])) as [Log Record]
		,[Operation]
		,UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("[RowLog Contents 0]") )', 'varchar(max)')) AS [RowLog Contents 0_var] --> New, added for reference; integrated below
		,UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("[RowLog Contents 1]") )', 'varchar(max)')) AS [RowLog Contents 1_var] --> New, added for reference; integrated below
	FROM #SysFnDblog A
	WHERE [AllocUnitId] IN (SELECT * FROM #AllocIdList)
		AND [Operation] IN ('LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS','LOP_INSERT_ROWS','LOP_DELETE_ROWS')
		AND [Context] IN ('LCX_HEAP','LCX_CLUSTERED')
		--AND [TRANSACTION ID] IN (SELECT [TRANSACTION ID] FROM #TransIdAllList) --> same filter for #SysFnDblog
	ORDER BY [Slot ID],[Transaction ID] DESC
--SELECT * FROM @PreliminaryRawData


/*
   If the [Operation] Type is 'LOP_MODIFY_ROW' then it is very simple to recover the modified data. The old data is in [RowLog Contents 0] Field and modified data is in [RowLog Contents 1] Field. Simply replace it with the modified data and get the old data.
   If the [Operation] Type is 'LOP_INSERT_ROWS' or 'LOP_DELETE_ROWS' then it is very simple to recover the data. The old data is in [RowLog Contents 0] Field.
*/
INSERT INTO @ModifiedRawData ([PAGE ID],[Slot ID],[AllocUnitId],[Datum],[TRANSACTION ID],[Transaction Name],[Current LSN],[Begin Time],[RowLog Contents 0_var],[Slot Info])
	SELECT A.[PAGE ID],A.[Slot ID],A.[AllocUnitId],'Restoration' AS [Datum],A.[TRANSACTION ID],A.[Transaction Name],A.[Current LSN],A.[Begin Time]
		,CASE WHEN A.[Operation] IN ('LOP_INSERT_ROWS','LOP_DELETE_ROWS') THEN
			(SELECT UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("A.[RowLog Contents 0]") )','varchar(max)'))) --> for INSERTs and/or DELETEs with no UPDATEs (@ModifiedRawData is empty when this update starts)
		ELSE --> look for the UPDATE mate from the previous record pull
			REPLACE(UPPER(B.[RowLog Contents 0_var]),UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("A.[RowLog Contents 1]") )','varchar(max)')),UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("A.[RowLog Contents 0]") )','varchar(max)')))
		END AS [RowLog Contents 0_var]
		,B.[Slot Info]
	FROM @PreliminaryRawData A
		LEFT JOIN @ModifiedRawData B ON B.[Current LSN]=A.[Current LSN]
	WHERE A.[Operation] IN ('LOP_MODIFY_ROW','LOP_INSERT_ROWS','LOP_DELETE_ROWS')

-- Convert the old data which is in string format to hex format (required).
UPDATE @ModifiedRawData SET [RowLog Contents 0] = CAST('' AS XML).value('xs:hexBinary(substring(sql:column("[RowLog Contents 0_var]"), 0) )', 'varbinary(max)')
	FROM @ModifiedRawData
	WHERE [RowLog Contents 0] IS NULL

IF (@Debug > 0) BEGIN
	select '@PreliminaryRawData'
	select * from @PreliminaryRawData
	print 'select * from @ModifiedRawData2_row_ops'
	select '@ModifiedRawData2_row_ops'
	select * from @ModifiedRawData
END
--> remove these records since their processing is complete
--DELETE FROM @PreliminaryRawData WHERE [Operation] IN ('LOP_MODIFY_ROW','LOP_INSERT_ROWS','LOP_DELETE_ROWS')
DELETE FROM @PreliminaryRawData WHERE [Operation] IN ('LOP_INSERT_ROWS','LOP_DELETE_ROWS')
IF (@Debug > 0) BEGIN
	print 'select * from @PreliminaryRawData_remaining'
	select '@PreliminaryRawData_remaining'
	select * from @PreliminaryRawData
END
 
/*******************CURSOR START*********************/
---Now we have modifed data plus its slot ID , page ID and allocunit as well.
--After that we need to get the old values before modfication, these data are in chunks.
IF (@Debug = 0) BEGIN
DECLARE Page_Data_Cursor CURSOR FOR
	SELECT 	[PAGE ID],[Slot ID],[AllocUnitId],[TRANSACTION ID],[Transaction Name],[Current LSN],[Begin Time],[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],[RowLog Contents 3],[RowLog Contents 4],[Log Record],[Operation]
		FROM @PreliminaryRawData
		WHERE [Operation] IN ('LOP_MODIFY_COLUMNS')
	ORDER BY [Slot ID],[TRANSACTION ID] DESC
END
ELSE
BEGIN
DECLARE Page_Data_Cursor CURSOR FOR
	SELECT 	[PAGE ID],[Slot ID],[AllocUnitId],[TRANSACTION ID],[Transaction Name],[Current LSN],[Begin Time],[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],[RowLog Contents 3],[RowLog Contents 4],[Log Record],[Operation]
		FROM @PreliminaryRawData
	ORDER BY [Slot ID],[TRANSACTION ID] DESC
END
 
/****************************************/

	OPEN Page_Data_Cursor
 
    FETCH NEXT FROM Page_Data_Cursor INTO @ConsolidatedPageID,@Slotid,@AllocUnitID,@TransactionID,@TransactionName,@CurrentLSN,@BeginTime,@RowLogContents0,@RowLogContents1,@RowLogContents2,@RowLogContents3,@RowLogContents4,@LogRecord,@Operation
    WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @Operation IN ('LOP_MODIFY_ROW','LOP_INSERT_ROWS','LOP_DELETE_ROWS')
		BEGIN
-- To see the debug printout, comment out the "DELETE FROM @PreliminaryRawData..." line above
IF (@Debug > 1) BEGIN
PRINT 'ConsolidatedPageID = '+@ConsolidatedPageID
PRINT 'Slotid = '+CONVERT(nvarchar,@Slotid)
PRINT 'AllocUnitID = '+CONVERT(nvarchar,@AllocUnitID)
PRINT 'TransactionID = '+@TransactionID
PRINT 'BeginTime = '+CONVERT(nvarchar,@BeginTime,121)
PRINT 'TransactionName = '+@TransactionName
DECLARE @RowLogContents0_var VARCHAR(MAX)
DECLARE @RowLogContents1_var VARCHAR(MAX)
SET @RowLogContents0_var=(SELECT UPPER(CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents0") )', 'varchar(max)')))
SET @RowLogContents1_var=(SELECT UPPER(CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents1") )', 'varchar(max)')))
PRINT 'RowLogContents0      = '+@RowLogContents0_var
PRINT 'RowLogContents1      = '+@RowLogContents1_var
IF (@TransactionName='UPDATE') BEGIN
DECLARE @RowLogContents0_var1 VARCHAR(MAX)
DECLARE @RowLogContents0_var2 VARCHAR(MAX)
DECLARE @RowLogContents1_var0 VARCHAR(MAX)
DECLARE @RowLogContents0_var0 VARCHAR(MAX)
SET @RowLogContents0_var1=(SELECT [RowLog Contents 0_var]
						FROM  @ModifiedRawData 
						WHERE [Current LSN]=@CurrentLSN AND [Datum]='Reference')
SET @RowLogContents1_var0=(SELECT UPPER(CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents1") )','varchar(max)'))
						FROM  @ModifiedRawData 
						WHERE [Current LSN]=@CurrentLSN AND [Datum]='Reference')
SET @RowLogContents0_var0=(SELECT UPPER(CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents0") )','varchar(max)'))
						FROM  @ModifiedRawData 
						WHERE [Current LSN]=@CurrentLSN AND [Datum]='Reference')
SET @RowLogContents0_var2=(SELECT REPLACE(UPPER([RowLog Contents 0_var]),UPPER(CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents1") )','varchar(max)')),UPPER(CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents0") )','varchar(max)')))
						FROM  @ModifiedRawData 
						WHERE [Current LSN]=@CurrentLSN AND [Datum]='Reference')
PRINT 'RowLogContents0_var1 = '+@RowLogContents0_var1
PRINT 'RowLogContents1_var0 = '+@RowLogContents1_var0
PRINT 'RowLogContents0_var0 = '+@RowLogContents0_var0
PRINT 'RowLogContents0_var2 = '+@RowLogContents0_var2
PRINT 'RowLogContents0_var2a= '+REPLACE(@RowLogContents0_var1,@RowLogContents1_var0,@RowLogContents0_var0)
END
PRINT ''
--DECLARE @LSN VARCHAR(50)
--DECLARE @LSN0 VARCHAR(50)
--DECLARE @LSN1 VARCHAR(50)
--DECLARE @LSN2 VARCHAR(50)
--DECLARE @LSN_int1 INTEGER
--DECLARE @LSN_int2 INTEGER
--SET @LSN=CONVERT(VARCHAR,@CurrentLSN)
--SET @LSN_int1=CHARINDEX(':',@LSN,1)
--SET @LSN_int2=CHARINDEX(':',@LSN,@LSN_int1+1)
--SET @LSN0=SUBSTRING(@LSN,1,@LSN_int1-1)
--SET @LSN1=SUBSTRING(@LSN,@LSN_int1+1,@LSN_int2-@LSN_int1-1)
--SET @LSN2=SUBSTRING(@LSN,@LSN_int2+1,LEN(@LSN))
--PRINT 'CurrentLSN='+@LSN0+':'+@LSN1+':'+@LSN2

--> SQL2005:CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),1,8))','varbinary(max)') == SQL2008:CONVERT(VARBINARY,SUBSTRING(@CurrentLSN,1,8),2)
--DECLARE @str VARCHAR(50)
--SET @str=CONVERT(VARCHAR,CONVERT(INT,CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),1,8))','varbinary(max)')))+':'+
--		CONVERT(VARCHAR,CONVERT(INT,CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),10,8))','varbinary(max)')))+':'+
--		CONVERT(VARCHAR,CONVERT(INT,CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),19,4))','varbinary(max)')))
--PRINT 'CurrentLSN_base10='+@str
--DECLARE @int BIGINT
--SET @int=CONVERT(BIGINT,CONVERT(VARCHAR,CONVERT(INT,CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),1,8))','varbinary(max)')))+
--		RIGHT('0000000000'+CONVERT(VARCHAR,CONVERT(INT,CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),10,8))','varbinary(max)'))),10)+
--		RIGHT('00000'+CONVERT(VARCHAR,CONVERT(INT,CAST('' as xml).value('xs:hexBinary(substring(sql:variable("@CurrentLSN"),19,4))','varbinary(max)'))),5) )
--PRINT 'CurrentLSN_int='+CONVERT(VARCHAR,@int)
END
		END
		
		IF @Operation ='LOP_MODIFY_COLUMNS'                   
		BEGIN

			/* If the @Operation Type is 'LOP_MODIFY_ROW' then we need to follow a different procedure to recover modified
			 .Because this time the data is also in chunks but merge with the data log.
			*/
			--First, we need to get the [RowLog Contents 3] Because in [Log Record] field the modified data is available after the [RowLog Contents 3] data.
			SET @RowLogContents3_Var = CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents3") )', 'varchar(max)')
			SET @LogRecord_Var = CAST('' AS XML).value('xs:hexBinary(sql:variable("@LogRecord"))', 'varchar(max)')

			---First get the modifed data chunks in string format 
			SET @RowLogData_Var = SUBSTRING(@LogRecord_Var, CHARINDEX(@RowLogContents3_Var,@LogRecord_Var)+LEN(@RowLogContents3_Var) ,LEN(@LogRecord_Var))
			--Then convert it into the hex values.
			SELECT @RowLogData_Hex = CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@RowLogData_Var"),0) )', 'varbinary(max)')
				FROM (SELECT CASE SUBSTRING(@RowLogData_Var, 1, 2)WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)

			-- Before recovering the modfied data we need to get the total fixed length data size and start position of the varaible data
			 
			SELECT @TotalFixedLengthData = CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , 2 + 1, 2)))) 
				,@VariableLength_Offset_Start = CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , 2 + 1, 2))))+5+CONVERT(INT, ceiling(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , 2 + 1, 2)))) + 1, 2))))/8.0))
			FROM @ModifiedRawData
			WHERE [Current LSN]=@CurrentLSN

			SET @FixedLength_Offset = CONVERT(BINARY(2),REVERSE(CONVERT(BINARY(4),(@RowLogContents0))))--)
			SET @VariableLength_Offset = CONVERT(INT,CONVERT(BINARY(2),REVERSE(@RowLogContents0)))
			 
			/* We already have modified data chunks in @RowLogData_Hex but this data is in merge format (modified plus actual data)
			  So , here we need [Row Log Contents 1] field , because in this field we have the data length both the modified and actual data
			   so this length will help us to break it into original and modified data chunks.
			*/
			SET @FixedLength_Loc = CONVERT(INT,SUBSTRING(@RowLogContents1,1,1))
			SET @VariableLength_Loc = CONVERT(INT,SUBSTRING(@RowLogContents1,3,1))

			/*First , we need to break Fix length data actual with the help of data length  */
			SET @OldFixedLengthStartPosition= CHARINDEX(@RowLogContents4,@RowLogData_Hex)
			SET @FixedOldValues = SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition,@FixedLength_Loc)
			SET @FixedLengthIncrease = (CASE WHEN (Len(@FixedOldValues)%4) = 0 THEN 1 ELSE (4-(LEN(@FixedOldValues)%4)) END)
			/*After that , we need to break Fix length data modified data with the help of data length  */
			SET @FixedNewValues =SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition+@FixedLength_Loc+@FixedLengthIncrease,@FixedLength_Loc) 

			/*Same we need to break the variable data with the help of data length*/
			SET @VariableOldValues =SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition+@FixedLength_Loc+@FixedLengthIncrease+@FixedLength_Loc+(@FixedLengthIncrease),@VariableLength_Loc)
			SET @VariableLengthIncrease = (CASE WHEN (LEN(@VariableOldValues)%4) = 0 THEN 1 ELSE (4-(Len(@VariableOldValues)%4))+1 END) 
			SET @VariableOldValues = (Case When @VariableLength_Loc = 1 Then @VariableOldValues+0x00 else @VariableOldValues end)

			SET @VariableNewValues = SUBSTRING(SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition+@FixedLength_Loc+@FixedLengthIncrease+@FixedLength_Loc+(@FixedLengthIncrease-1)+@VariableLength_Loc+@VariableLengthIncrease,Len(@RowLogData_Hex)+1),1,Len(@RowLogData_Hex)+1) --LEN(@VariableOldValues)

			/*here we need to replace the fixed length &  variable length actaul data with modifed data 
			*/

			SELECT @VariableNewValues=CASE
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)+1),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)+1)
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)  ),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues))
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)-1),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)-1)--3 --Substring(@VariableNewValues,0,Len(@VariableNewValues)-1)
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)-2),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)-2)
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)-3),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)-3) --5--Substring(@VariableNewValues,0,Len(@VariableNewValues)-3)
				END
			FROM @ModifiedRawData
			WHERE [Current LSN]=@CurrentLSN

			INSERT INTO @ModifiedRawData ([PAGE ID],[Slot ID],[AllocUnitId],[Datum],[TRANSACTION ID],[Transaction Name],[Current LSN],[Begin Time],[RowLog Contents 0_var],[RowLog Contents 0],[Slot Info]) 
				SELECT @ConsolidatedPageID AS [PAGE ID],@Slotid AS [Slot ID],@AllocUnitID AS [AllocUnitId],'Restoration' AS [Datum],@TransactionID AS [TRANSACTION ID],@TransactionName AS [Transaction Name],@CurrentLSN AS [Current LSN],@BeginTime AS [Begin Time]
					,NULL --> [RowLog Contents 0_var]
					,ISNULL( --> due to chained updates; this only works on the most recent update
						CAST(REPLACE(SUBSTRING([RowLog Contents 0],0,@TotalFixedLengthData+1),@FixedNewValues, @FixedOldValues) AS VARBINARY(max))
							+ SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 1, 2)
							+ SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 3, CONVERT(INT, ceiling(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 1, 2))))/8.0)))
							+ SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 3 + CONVERT(INT, ceiling(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 1, 2))))/8.0)), 2)
							+ SUBSTRING([RowLog Contents 0],@VariableLength_Offset_Start,(@VariableLength_Offset-(@VariableLength_Offset_Start-1)))
							+ CAST(REPLACE(SUBSTRING([RowLog Contents 0],@VariableLength_Offset+1,Len(@VariableNewValues))
									,@VariableNewValues,@VariableOldValues) AS VARBINARY) 
							+ Substring([RowLog Contents 0],@VariableLength_Offset+Len(@VariableNewValues)+1,LEN([RowLog Contents 0]))
						,[RowLog Contents 0]) --> [RowLog Contents 0]
					,[Slot Info]
				FROM @ModifiedRawData
				WHERE [Current LSN]=@CurrentLSN

			--> Convert the old data which is in hex format to string format (not required).
			UPDATE @ModifiedRawData SET [RowLog Contents 0_var] = CAST('' AS XML).value('xs:hexBinary(sql:column("[RowLog Contents 0]") )', 'varchar(max)')
			FROM @ModifiedRawData 
			WHERE [Current LSN]=@CurrentLSN
		END

		FETCH NEXT FROM Page_Data_Cursor INTO @ConsolidatedPageID,@Slotid,@AllocUnitID,@TransactionID,@TransactionName,@CurrentLSN,@BeginTime,@RowLogContents0,@RowLogContents1,@RowLogContents2,@RowLogContents3,@RowLogContents4,@LogRecord,@Operation
	END
 
CLOSE Page_Data_Cursor
DEALLOCATE Page_Data_Cursor
/*******************CURSOR END*********************/
------------------------------------------------------------------------------------------------------------
DECLARE @ModRawDataLoop INT
--> Add sequence index number(s)
UPDATE A SET [EntryID] = B.[index] FROM @ModifiedRawData A 
	JOIN (SELECT ROW_NUMBER() OVER (ORDER BY [Current LSN],[Datum] DESC) AS [index], [ID] FROM @ModifiedRawData) B ON
		 B.[ID] = A.[ID]
--> Add backward reference(s) and forward reference(s)
UPDATE A SET
	[PrevEntry] =
		ISNULL((SELECT MAX(B.[EntryID]) AS [EntryID] FROM @ModifiedRawData B
				WHERE B.[Slot ID] = A.[Slot ID] AND B.[EntryID] < A.[EntryID]),0)
	,[NextEntry] =
		ISNULL((SELECT MIN(B.[EntryID]) AS [EntryID] FROM @ModifiedRawData B
				WHERE B.[Slot ID] = A.[Slot ID] AND B.[EntryID] > A.[EntryID]),0)
	FROM @ModifiedRawData A
--> Add chain level(s)
UPDATE A SET [ChainLevel] = B.[index] FROM @ModifiedRawData A 
	JOIN (SELECT DENSE_RANK() OVER (PARTITION BY [SLOT ID] ORDER BY [Current LSN])-1 AS [index], [ID] FROM @ModifiedRawData) B ON 
		B.[ID] = A.[ID]
--> Set the process order(s) --> the reverse of the [ChainLevel]
UPDATE A SET
	[Process] = CASE WHEN ([ChainLevel] = 0) THEN 0
		ELSE (SELECT MAX([ChainLevel]) FROM @ModifiedRawData B WHERE B.[Slot ID] = A.[Slot ID]) - [ChainLevel]
		END
	FROM @ModifiedRawData A
--select * from @ModifiedRawData order by [EntryID]

--> Get the chained UPDATE records (forward in time) to locate the replaced "Reference" record(s) (chained UPDATEs)
SELECT B.* INTO #T1
	FROM @ModifiedRawData A
	INNER JOIN @ModifiedRawData B ON B.[Transaction Name] = 'UPDATE'
		AND B.[NextEntry] = A.[EntryID]
		AND B.[Datum] = 'Reference' AND A.[Datum] = 'Restoration'
		--AND B.[Current LSN] <> A.[Current LSN] --> should always be true
SET @ModRawDataLoop = @@ROWCOUNT
--select * from #T1

IF (@ModRawDataLoop > 0) BEGIN
	WHILE (@ModRawDataLoop > 0) BEGIN
		/*
			If the [Operation] Type is 'LOP_MODIFY_ROW' then it is very simple to recover the modified data. The old data is in [RowLog Contents 0] Field and modified data is in [RowLog Contents 1] Field. Simply replace it with the modified data and get the old data.
		*/
		--> First, change the byte of [RowLog Contents 0_var] that may differ from the first byte of [RowLog Contents 1_var] (I don't know why these don't match sometimes)
		--> Fix the copy before it is restored back into the applicable records (next)
		UPDATE #T1 SET
				[RowLog Contents 0_var] =
					SUBSTRING(B.[RowLog Contents 0_var],1,PATINDEX('%'+SUBSTRING(A.[RowLog Contents 1_var],3,LEN(A.[RowLog Contents 1_var]))+'%',B.[RowLog Contents 0_var])-3)
					+SUBSTRING(A.[RowLog Contents 1_var],1,2)
					+SUBSTRING(B.[RowLog Contents 0_var],PATINDEX('%'+SUBSTRING(A.[RowLog Contents 1_var],3,LEN(A.[RowLog Contents 1_var]))+'%',B.[RowLog Contents 0_var]),LEN(B.[RowLog Contents 0_var]))
				,[RowLog Contents 0] =
					SUBSTRING(B.[RowLog Contents 0],1,PATINDEX('%'+SUBSTRING(A.[RowLog Contents 1_var],3,LEN(A.[RowLog Contents 1_var]))+'%',B.[RowLog Contents 0_var])/2-1)
					+SUBSTRING(A.[RowLog Contents 1],1,1)
					+SUBSTRING(B.[RowLog Contents 0],PATINDEX('%'+SUBSTRING(A.[RowLog Contents 1_var],3,LEN(A.[RowLog Contents 1_var]))+'%',B.[RowLog Contents 0_var])/2+1,LEN(B.[RowLog Contents 0]))
			FROM @PreliminaryRawData A
				INNER JOIN #T1 C ON C.[Current LSN] = A.[Current LSN]
				INNER JOIN @ModifiedRawData B ON B.[EntryID] = C.[NextEntry] --> source location
			WHERE A.[Operation] IN ('LOP_MODIFY_ROW') AND C.[Process] = 1
		--select * from #T1
		--> Restore (one level) the replaced (and therefore lost) "Reference" information (chained UPDATEs)
		-- for both the "Reference" and "Restoration" records, and the original source record
		UPDATE @ModifiedRawData SET
				[RowLog Contents 0_var] = B.[RowLog Contents 0_var]
			,[RowLog Contents 0] = B.[RowLog Contents 0]
			FROM @ModifiedRawData A
				INNER JOIN #T1 B ON A.[EntryID] IN (B.[EntryID],B.[PrevEntry],B.[NextEntry])
			WHERE B.[Process] = 1
		--select * from @ModifiedRawData order by [EntryID]
		--> Now, the substitution will work, so update [RowLog Contents 0_var]
		UPDATE @ModifiedRawData SET
				[RowLog Contents 0_var] =
					--> look for the UPDATE mate from the previous record pull
					REPLACE(UPPER(B.[RowLog Contents 0_var]),UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("A.[RowLog Contents 1]") )','varchar(max)')),UPPER(CAST('' AS XML).value('xs:hexBinary(sql:column("A.[RowLog Contents 0]") )','varchar(max)')))
				,[RowLog Contents 0] = NULL
			FROM @PreliminaryRawData A
				INNER JOIN @ModifiedRawData B ON B.[Current LSN]=A.[Current LSN]
				INNER JOIN #T1 C ON C.[PrevEntry] = B.[EntryID]
			WHERE A.[Operation] IN ('LOP_MODIFY_ROW') AND B.[Process] = 1

		-- Convert the old data which is in string format to hex format (required).
		UPDATE @ModifiedRawData SET
			[RowLog Contents 0] = CAST('' AS XML).value('xs:hexBinary(substring(sql:column("[RowLog Contents 0_var]"), 0) )', 'varbinary(max)')
			FROM @ModifiedRawData
			WHERE [RowLog Contents 0] IS NULL
		SET @ModRawDataLoop = @@ROWCOUNT
		--print '@ModRawDataLoop (LOP_MODIFY_ROW)= '+CONVERT(varchar,@ModRawDataLoop)
		--select * from @ModifiedRawData order by [EntryID]

/*******************CURSOR START*********************/
---Now we have modifed data plus its slot ID , page ID and allocunit as well.
--After that we need to get the old values before modfication, these data are in chunks.
DECLARE Page_Data_Cursor CURSOR FOR
	SELECT 	A.[PAGE ID],A.[Slot ID],A.[AllocUnitId],A.[TRANSACTION ID],A.[Transaction Name],A.[Current LSN],A.[Begin Time],A.[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],[RowLog Contents 3],[RowLog Contents 4],[Log Record],[Operation]
		FROM @PreliminaryRawData A
			INNER JOIN @ModifiedRawData B ON B.[Current LSN] = A.[Current LSN] --> skip this section if there are no updates
		WHERE [Operation] IN ('LOP_MODIFY_COLUMNS') AND B.[Process] = 1 AND B.[Datum] = 'Restoration'
	ORDER BY A.[Slot ID],A.[TRANSACTION ID] DESC

SET @ModRawDataLoop = 0
/****************************************/

OPEN Page_Data_Cursor
 
    FETCH NEXT FROM Page_Data_Cursor INTO @ConsolidatedPageID,@Slotid,@AllocUnitID,@TransactionID,@TransactionName,@CurrentLSN,@BeginTime,@RowLogContents0,@RowLogContents1,@RowLogContents2,@RowLogContents3,@RowLogContents4,@LogRecord,@Operation
    WHILE @@FETCH_STATUS = 0
	BEGIN
		--IF @Operation ='LOP_MODIFY_COLUMNS'                   
		BEGIN

			/* If the @Operation Type is 'LOP_MODIFY_ROW' then we need to follow a different procedure to recover modified
			 .Because this time the data is also in chunks but merge with the data log.
			*/
			--First, we need to get the [RowLog Contents 3] Because in [Log Record] field the modified data is available after the [RowLog Contents 3] data.
			SET @RowLogContents3_Var = CAST('' AS XML).value('xs:hexBinary(sql:variable("@RowLogContents3") )', 'varchar(max)')
			SET @LogRecord_Var = CAST('' AS XML).value('xs:hexBinary(sql:variable("@LogRecord"))', 'varchar(max)')

			---First get the modifed data chunks in string format 
			SET @RowLogData_Var = SUBSTRING(@LogRecord_Var, CHARINDEX(@RowLogContents3_Var,@LogRecord_Var)+LEN(@RowLogContents3_Var) ,LEN(@LogRecord_Var))
			--Then convert it into the hex values.
			SELECT @RowLogData_Hex = CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@RowLogData_Var"),0) )', 'varbinary(max)')
				FROM (SELECT CASE SUBSTRING(@RowLogData_Var, 1, 2)WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)

			-- Before recovering the modfied data we need to get the total fixed length data size and start position of the varaible data
			 
			SELECT @TotalFixedLengthData = CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , 2 + 1, 2)))) 
				,@VariableLength_Offset_Start = CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , 2 + 1, 2))))+5+CONVERT(INT, ceiling(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0] , 2 + 1, 2)))) + 1, 2))))/8.0))
			FROM @ModifiedRawData
			WHERE [Current LSN]=@CurrentLSN

			SET @FixedLength_Offset = CONVERT(BINARY(2),REVERSE(CONVERT(BINARY(4),(@RowLogContents0))))--)
			SET @VariableLength_Offset = CONVERT(INT,CONVERT(BINARY(2),REVERSE(@RowLogContents0)))
			 
			/* We already have modified data chunks in @RowLogData_Hex but this data is in merge format (modified plus actual data)
			  So , here we need [Row Log Contents 1] field , because in this field we have the data length both the modified and actual data
			   so this length will help us to break it into original and modified data chunks.
			*/
			SET @FixedLength_Loc = CONVERT(INT,SUBSTRING(@RowLogContents1,1,1))
			SET @VariableLength_Loc = CONVERT(INT,SUBSTRING(@RowLogContents1,3,1))

			/*First , we need to break Fix length data actual with the help of data length  */
			SET @OldFixedLengthStartPosition= CHARINDEX(@RowLogContents4,@RowLogData_Hex)
			SET @FixedOldValues = SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition,@FixedLength_Loc)
			SET @FixedLengthIncrease = (CASE WHEN (Len(@FixedOldValues)%4) = 0 THEN 1 ELSE (4-(LEN(@FixedOldValues)%4)) END)
			/*After that , we need to break Fix length data modified data with the help of data length  */
			SET @FixedNewValues =SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition+@FixedLength_Loc+@FixedLengthIncrease,@FixedLength_Loc) 

			/*Same we need to break the variable data with the help of data length*/
			SET @VariableOldValues =SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition+@FixedLength_Loc+@FixedLengthIncrease+@FixedLength_Loc+(@FixedLengthIncrease),@VariableLength_Loc)
			SET @VariableLengthIncrease = (CASE WHEN (LEN(@VariableOldValues)%4) = 0 THEN 1 ELSE (4-(Len(@VariableOldValues)%4))+1 END) 
			SET @VariableOldValues = (Case When @VariableLength_Loc = 1 Then @VariableOldValues+0x00 else @VariableOldValues end)

			SET @VariableNewValues = SUBSTRING(SUBSTRING(@RowLogData_Hex,@OldFixedLengthStartPosition+@FixedLength_Loc+@FixedLengthIncrease+@FixedLength_Loc+(@FixedLengthIncrease-1)+@VariableLength_Loc+@VariableLengthIncrease,Len(@RowLogData_Hex)+1),1,Len(@RowLogData_Hex)+1) --LEN(@VariableOldValues)

			/*here we need to replace the fixed length &  variable length actaul data with modifed data 
			*/

			SELECT @VariableNewValues=CASE
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)+1),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)+1)
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)  ),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues))
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)-1),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)-1)--3 --Substring(@VariableNewValues,0,Len(@VariableNewValues)-1)
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)-2),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)-2)
				WHEN Charindex(Substring(@VariableNewValues,0,Len(@VariableNewValues)-3),[RowLog Contents 0])<>0 Then Substring(@VariableNewValues,0,Len(@VariableNewValues)-3) --5--Substring(@VariableNewValues,0,Len(@VariableNewValues)-3)
				END
			FROM @ModifiedRawData
			WHERE [Current LSN]=@CurrentLSN

			UPDATE @ModifiedRawData SET
				[RowLog Contents 0] =
					CAST(REPLACE(SUBSTRING([RowLog Contents 0],0,@TotalFixedLengthData+1),@FixedNewValues, @FixedOldValues) AS VARBINARY(max))
						+ SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 1, 2)
						+ SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 3, CONVERT(INT, ceiling(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 1, 2))))/8.0)))
						+ SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 3 + CONVERT(INT, ceiling(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], @TotalFixedLengthData + 1, 2))))/8.0)), 2)
						+ SUBSTRING([RowLog Contents 0],@VariableLength_Offset_Start,(@VariableLength_Offset-(@VariableLength_Offset_Start-1)))
						+ CAST(REPLACE(SUBSTRING([RowLog Contents 0],@VariableLength_Offset+1,Len(@VariableNewValues))
								,@VariableNewValues,@VariableOldValues) AS VARBINARY) 
						+ Substring([RowLog Contents 0],@VariableLength_Offset+Len(@VariableNewValues)+1,LEN([RowLog Contents 0]))
				,[RowLog Contents 0_var] = NULL
				FROM @ModifiedRawData
				WHERE [Current LSN]=@CurrentLSN AND [Process] = 1 AND [Datum] = 'Restoration'

			--> Convert the old data which is in hex format to string format (not required).
			UPDATE @ModifiedRawData SET [RowLog Contents 0_var] = CAST('' AS XML).value('xs:hexBinary(sql:column("[RowLog Contents 0]") )', 'varchar(max)')
				FROM @ModifiedRawData 
				WHERE [RowLog Contents 0_var] IS NULL
			SET @ModRawDataLoop = @ModRawDataLoop + @@ROWCOUNT

		END

		FETCH NEXT FROM Page_Data_Cursor INTO @ConsolidatedPageID,@Slotid,@AllocUnitID,@TransactionID,@TransactionName,@CurrentLSN,@BeginTime,@RowLogContents0,@RowLogContents1,@RowLogContents2,@RowLogContents3,@RowLogContents4,@LogRecord,@Operation
	END
	--print '@ModRawDataLoop (LOP_MODIFY_COLUMNS)= '+CONVERT(varchar,@ModRawDataLoop)
 
CLOSE Page_Data_Cursor
DEALLOCATE Page_Data_Cursor
/*******************CURSOR END*********************/
--select * from @ModifiedRawData order by [EntryID]

		--> Update the process order
		UPDATE #T1 SET [Process] = [Process] - 1
			FROM #T1
			WHERE [Process] > 0
		SET @ModRawDataLoop = (SELECT COUNT(*) FROM #T1 WHERE [Process] > 0)
		--print '@ModRawDataLoop (remaining)= '+CONVERT(varchar,@ModRawDataLoop)

		IF (@ModRawDataLoop > 0) BEGIN
			--> Update the process order
			UPDATE @ModifiedRawData SET [Process] = [Process] - 1
				FROM @ModifiedRawData
				WHERE [Process] > 0

			--> Now look ahead and get (temporarily store) the replaced "Reference" information (chained UPDATEs)
			-- this is required for UPDATEs occurring before the most recent UPDATE
			UPDATE #T1 SET --> redundant the for the first level
					 [RowLog Contents 0_var] = A.[RowLog Contents 0_var]
					,[RowLog Contents 0] = A.[RowLog Contents 0]
				FROM @ModifiedRawData A
					INNER JOIN #T1 B ON B.[NextEntry] = A.[EntryID]
				WHERE B.[Process] = 1
			--select * from #T1
		END
	END
END
------------------------------------------------------------------------------------------------------------
IF (@Debug > 0) BEGIN
	print 'select * from @ModifiedRawData2'
	select '@ModifiedRawData2'
	select * from @ModifiedRawData ORDER BY [Current LSN], [Datum] Desc
END

DECLARE @RowLogContents VARBINARY(8000)
DECLARE @AllocUnitName NVARCHAR(MAX)
DECLARE @SQL NVARCHAR(MAX)
 
DECLARE @bitTable TABLE(
	 [ID]       INT
	,[Bitvalue] INT
	)
--Create table to set the bit position of one byte.
 
INSERT INTO @bitTable
	SELECT 0,2  UNION ALL
	SELECT 1,2  UNION ALL
	SELECT 2,4  UNION ALL
	SELECT 3,8  UNION ALL
	SELECT 4,16 UNION ALL
	SELECT 5,32 UNION ALL
	SELECT 6,64 UNION ALL
	SELECT 7,128
 
--Create table to collect the row data.
DECLARE @DeletedRecords TABLE(
	 [ID] INT IDENTITY(1,1)
	,[RowLogContents]    VARBINARY(8000)
	,[AllocUnitID]       BIGINT
	,[TransactionID]     NVARCHAR(Max)
	,[Slot ID]           INT
	,[FixedLengthData]   SMALLINT
	,[TotalNoOfCols]     SMALLINT
	,[NullBitMapLength]  SMALLINT
	,[NullBytes]         VARBINARY(8000)
	,[TotalNoOfVarCols]  SMALLINT
	,[ColumnOffsetArray] VARBINARY(8000)
	,[VarColumnStart]    SMALLINT
	,[NullBitMap]        VARCHAR(MAX)
	,[TRANSACTION ID]    NVARCHAR(Max)
	,[Transaction Name]  NVARCHAR(MAX)
	,[Datum]             VARCHAR(20)
	,[Current LSN]       VARCHAR(MAX)
	,[Begin Time]        DATETIME
	)
--Create a common table expression to get all the row data plus how many bytes we have for each row.
;WITH RowData AS
	(SELECT
		[RowLog Contents 0] AS [RowLogContents] 
		,[AllocUnitID] 
		,[ID] AS [TransactionID]  
		,[Slot ID] as [Slot ID]
		--[Fixed Length Data] = Substring (RowLog content 0, Status Bit A+ Status Bit B + 1,2 bytes)
		,CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) AS [FixedLengthData]  --@FixedLengthData

		 --[TotalnoOfCols] =  Substring (RowLog content 0, [Fixed Length Data] + 1,2 bytes)
		,CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
			REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2)))) as  [TotalNoOfCols]

		--[NullBitMapLength]=ceiling([Total No of Columns] /8.0)
		,CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
			REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0)) as [NullBitMapLength] 
 
		--[Null Bytes] = Substring (RowLog content 0, Status Bit A+ Status Bit B + [Fixed Length Data] +1, [NullBitMapLength] )
		,SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 3,
			CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
			REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0))) as [NullBytes]
 
		--[TotalNoOfVarCols] = Substring (RowLog content 0, Status Bit A+ Status Bit B + [Fixed Length Data] +1, [Null Bitmap length] + 2 )
		,(CASE WHEN SUBSTRING([RowLog Contents 0], 1, 1) In (0x30,0x70)
		THEN CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0],
			CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 3 +
			CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
			REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0)), 2))))
		ELSE null END) AS [TotalNoOfVarCols] 
 
		--[ColumnOffsetArray]= Substring (RowLog content 0, Status Bit A+ Status Bit B + [Fixed Length Data] +1, [Null Bitmap length] + 2 , [TotalNoOfVarCols]*2 )
		,(CASE WHEN SUBSTRING([RowLog Contents 0], 1, 1) In (0x30,0x70)
		THEN SUBSTRING([RowLog Contents 0],
			CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 3 +
			CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
			REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0)) + 2,
			(CASE WHEN SUBSTRING([RowLog Contents 0], 1, 1) In (0x30,0x70)
			THEN CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0],
				CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 3 +
				CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
				REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0)), 2))))
			ELSE null END) * 2)
		ELSE null END) AS [ColumnOffsetArray] 
 
		--  Variable column Start = Status Bit A+ Status Bit B + [Fixed Length Data] + [Null Bitmap length] + 2+([TotalNoOfVarCols]*2)
		,CASE WHEN SUBSTRING([RowLog Contents 0], 1, 1)In (0x30,0x70)
		THEN (CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 4 +
			CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
			REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0)) +
			((CASE WHEN SUBSTRING([RowLog Contents 0], 1, 1) In (0x30,0x70)
			THEN CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0],
				CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 3 +
				CONVERT(INT, CEILING(CONVERT(INT, CONVERT(BINARY(2), REVERSE(SUBSTRING([RowLog Contents 0], CONVERT(SMALLINT, CONVERT(BINARY(2),
				REVERSE(SUBSTRING([RowLog Contents 0], 2 + 1, 2)))) + 1, 2))))/8.0)), 2))))
			ELSE null END) * 2)) 
		ELSE NULL END AS [VarColumnStart]
		,[TRANSACTION ID]
		,[Transaction Name]
		,[Datum]
		,[Current LSN]
		,[Begin Time]
	FROM @ModifiedRawData
	)
 
	---Use this technique to repeat the row till the no of bytes of the row.
	,N1 (n) AS (SELECT 1 UNION ALL SELECT 1)
	,N2 (n) AS (SELECT 1 FROM N1 AS X, N1 AS Y)
	,N3 (n) AS (SELECT 1 FROM N2 AS X, N2 AS Y)
	,N4 (n) AS (SELECT ROW_NUMBER() OVER(ORDER BY X.n) FROM N3 AS X, N3 AS Y)

	INSERT INTO @DeletedRecords
		SELECT [RowLogContents]
			,[AllocUnitID]
			,[TransactionID]
			,[Slot ID]
			,[FixedLengthData]
			,[TotalNoOfCols]
			,[NullBitMapLength]
			,[NullBytes]
			,[TotalNoOfVarCols]
			,[ColumnOffsetArray]
			,[VarColumnStart]
			 --Get the Null value against each column (1 means null zero means not null)
			,[NullBitMap]=(REPLACE(STUFF(
				(SELECT ',' +
					(CASE WHEN [ID]=0 THEN CONVERT(NVARCHAR(1),(SUBSTRING(NullBytes, n, 1) % 2)) ELSE CONVERT(NVARCHAR(1),((SUBSTRING(NullBytes, n, 1) / [Bitvalue]) % 2)) END) --as [nullBitMap]
				FROM N4 AS Nums
					JOIN RowData AS C ON n <= NullBitMapLength
					CROSS JOIN @bitTable WHERE C.[RowLogContents] = D.[RowLogContents]
				ORDER BY [RowLogContents],n ASC FOR XML PATH('')
				),1,1,''),',',''))
			,[TRANSACTION ID]
			,[Transaction Name]
			,[Datum]
			,[Current LSN]
			,[Begin Time]
		FROM RowData D
IF (@Debug > 0) BEGIN
	print 'select * from @DeletedRecords'
	select '@DeletedRecords'
	select * from @DeletedRecords
END

CREATE TABLE [#temp_Data](
	 [FieldName]        VARCHAR(MAX) COLLATE database_default NOT NULL
	,[FieldValue]       VARCHAR(MAX) COLLATE database_default NULL
	,[FieldSetValue]    VARCHAR(MAX) COLLATE database_default NULL
	,[Rowlogcontents]   VARBINARY(8000)
	,[TransactionID]    VARCHAR(MAX) COLLATE database_default NOT NULL
	,[Slot ID]          INT
	,[NonID]            INT
	,[nullbit]          INT
	,[TRANSACTION ID]   NVARCHAR(MAX)
	,[Transaction Name] NVARCHAR(MAX)
	,[Datum]            VARCHAR(20)
	,[Current LSN]      VARCHAR(MAX)
	,[Begin Time]       DATETIME
	--,[System_type_id] INT
	)
--Create common table expression and join it with the rowdata table
--to get each column details
;With CTE AS
	/*This part is for variable data columns*/
	(SELECT
		 A.[ID]
		,[Rowlogcontents]
		,[TransactionID]
		,[Slot ID]
		,[TRANSACTION ID]
		,[Transaction Name]
		,[Datum]
		,[Current LSN]
		,[Begin Time]
		,[NAME]
		,cols.leaf_null_bit AS nullbit
		,leaf_offset
		,ISNULL(syscolumns.length, cols.max_length) AS [length]
		,cols.system_type_id
		,cols.leaf_bit_position AS bitpos
		,ISNULL(syscolumns.xprec, cols.precision) AS xprec
		,ISNULL(syscolumns.xscale, cols.scale) AS xscale
		,SUBSTRING([nullBitMap], cols.leaf_null_bit, 1) AS is_null
		--Calculate the variable column size from the variable column offset array
		,(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
			THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
			ELSE 0 END
		) AS [Column value Size]
 
		--Calculate the column length
		,(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
			THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING ([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
				- ISNULL(NULLIF(CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE (SUBSTRING([ColumnOffsetArray], (2*((leaf_offset*-1)-1))-1, 2)))), 0), [varColumnStart]) --> If Null OR zero
			ELSE 0 END
		) AS [Column Length]
 
		--Get the Hexa decimal value from the RowlogContent
		--HexValue of the variable column=Substring([Column value Size] - [Column Length] + 1,[Column Length])
		--This is the data of your column but in the Hexvalue
		,CASE WHEN SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=1
			THEN NULL
			ELSE SUBSTRING(Rowlogcontents,
				(
				(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
					THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
					ELSE 0 END
				)
				-(
				(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
					THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
						- ISNULL(NULLIF(CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*((leaf_offset*-1)-1))-1, 2)))), 0), [varColumnStart]) --> If Null OR zero
					ELSE 0 END
				))
				)+1,(
				(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
					THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
						- ISNULL(NULLIF(CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*((leaf_offset*-1)-1))-1, 2)))), 0), [varColumnStart]) --> If Null OR zero
					ELSE 0 END
				))
				)
			END AS hex_Value
 
	FROM @DeletedRecords A
		INNER JOIN sys.allocation_units allocunits ON
			A.[AllocUnitId]=allocunits.[Allocation_Unit_Id]
		INNER JOIN sys.partitions partitions ON
			(allocunits.type IN (1, 3) AND partitions.hobt_id = allocunits.container_id)
			OR (allocunits.type = 2 AND partitions.partition_id = allocunits.container_id)
		INNER JOIN sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id
		LEFT OUTER JOIN syscolumns ON syscolumns.id = partitions.object_id AND syscolumns.colid = cols.partition_column_id
	WHERE leaf_offset < 0
 
	UNION
	/*This part is for fixed data columns*/
	SELECT 
		 A.[ID]
		,[Rowlogcontents]
		,[TransactionID]
		,[Slot ID]
		,[TRANSACTION ID]
		,[Transaction Name]
		,[Datum]
		,[Current LSN]
		,[Begin Time]
		,[NAME]
		,cols.leaf_null_bit AS [nullbit]
		,leaf_offset
		,ISNULL(syscolumns.length, cols.max_length) AS [length]
		,cols.system_type_id
		,cols.leaf_bit_position AS bitpos
		,ISNULL(syscolumns.xprec, cols.precision) AS xprec
		,ISNULL(syscolumns.xscale, cols.scale) AS xscale
		,SUBSTRING([nullBitMap], cols.leaf_null_bit, 1) AS is_null
		,(SELECT TOP 1 ISNULL(SUM(CASE WHEN C.leaf_offset >1 THEN max_length ELSE 0 END),0)
		FROM sys.system_internals_partition_columns C
		WHERE cols.partition_id=C.partition_id AND C.leaf_null_bit<cols.leaf_null_bit
		)+5 AS [Column value Size]
		,syscolumns.length AS [Column Length]
 
		,CASE WHEN SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=1
			THEN NULL
			ELSE SUBSTRING(Rowlogcontents,
				(SELECT TOP 1 ISNULL(SUM(CASE WHEN C.leaf_offset >1 THEN max_length ELSE 0 END),0)
				FROM sys.system_internals_partition_columns C
				WHERE cols.partition_id =C.partition_id AND C.leaf_null_bit<cols.leaf_null_bit
				)+5
				,syscolumns.length)
			END AS hex_Value
	FROM @DeletedRecords A
		INNER JOIN sys.allocation_units allocunits ON
			A.[AllocUnitId]=allocunits.[Allocation_Unit_Id]
		INNER JOIN sys.partitions partitions ON
			(allocunits.type IN (1, 3) AND partitions.hobt_id = allocunits.container_id)
			OR (allocunits.type = 2 AND partitions.partition_id = allocunits.container_id)
		INNER JOIN sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id
		LEFT OUTER JOIN syscolumns ON syscolumns.id = partitions.object_id AND syscolumns.colid = cols.partition_column_id
	WHERE leaf_offset>0
	)
	--Converting data from Hexvalue to its orgional datatype.
	--Implemented datatype conversion mechanism for each datatype
	--Select * from sys.columns Where [object_id]=object_id('' + @SchemaName_n_TableName + '')
	--Select * from CTE
	INSERT INTO #temp_Data
		SELECT
			NAME,
			CASE
				WHEN system_type_id IN (231, 239) THEN  LTRIM(RTRIM(CONVERT(NVARCHAR(max),hex_Value)))  --NVARCHAR ,NCHAR
				WHEN system_type_id IN (167,175) THEN  LTRIM(RTRIM(CONVERT(VARCHAR(max),REPLACE(hex_Value, 0x00, 0x20))))  --VARCHAR,CHAR
				WHEN system_type_id = 48 THEN CONVERT(VARCHAR(MAX), CONVERT(TINYINT, CONVERT(BINARY(1), REVERSE (hex_Value)))) --TINY INTEGER
				WHEN system_type_id = 52 THEN CONVERT(VARCHAR(MAX), CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE (hex_Value)))) --SMALL INTEGER
				WHEN system_type_id = 56 THEN CONVERT(VARCHAR(MAX), CONVERT(INT, CONVERT(BINARY(4), REVERSE(hex_Value)))) -- INTEGER
				WHEN system_type_id = 127 THEN CONVERT(VARCHAR(MAX), CONVERT(BIGINT, CONVERT(BINARY(8), REVERSE(hex_Value))))-- BIG INTEGER
				WHEN system_type_id = 61 Then CONVERT(VARCHAR(MAX),CONVERT(DATETIME,CONVERT(VARBINARY(8000),REVERSE (hex_Value))),100) --DATETIME
				--WHEN system_type_id IN( 40) Then CONVERT(VARCHAR(MAX),CONVERT(DATE,CONVERT(VARBINARY(8000),(hex_Value))),100) --DATE This datatype only works for SQL Server 2008
				WHEN system_type_id =58 Then CONVERT(VARCHAR(MAX),CONVERT(SMALLDATETIME,CONVERT(VARBINARY(8000),REVERSE(hex_Value))),100) --SMALL DATETIME
				WHEN system_type_id = 108 THEN CONVERT(VARCHAR(MAX), CAST(CONVERT(NUMERIC(38,30), CONVERT(VARBINARY,CONVERT(VARBINARY,xprec)+CONVERT(VARBINARY,xscale))+CONVERT(VARBINARY(1),0) + hex_Value) as FLOAT)) --- NUMERIC
				WHEN system_type_id In(60,122) THEN CONVERT(VARCHAR(MAX),Convert(MONEY,Convert(VARBINARY(8000),Reverse(hex_Value))),2) --MONEY,SMALLMONEY
				WHEN system_type_id =106 THEN CONVERT(VARCHAR(MAX), CAST(CONVERT(Decimal(38,34), CONVERT(VARBINARY,Convert(VARBINARY,xprec)+CONVERT(VARBINARY,xscale))+CONVERT(VARBINARY(1),0) + hex_Value) as FLOAT)) --- DECIMAL
				WHEN system_type_id = 104 THEN CONVERT(VARCHAR(MAX),CONVERT (BIT,CONVERT(BINARY(1), hex_Value)%2))  -- BIT
				WHEN system_type_id =62 THEN  RTRIM(LTRIM(STR(CONVERT(FLOAT,SIGN(CAST(CONVERT(VARBINARY(8000),Reverse(hex_Value)) AS BIGINT)) * (1.0 + (CAST(CONVERT(VARBINARY(8000),Reverse(hex_Value)) AS BIGINT) & 0x000FFFFFFFFFFFFF) * POWER(CAST(2 AS FLOAT), -52)) * POWER(CAST(2 AS FLOAT),((CAST(CONVERT(VARBINARY(8000),Reverse(hex_Value)) AS BIGINT) & 0x7ff0000000000000) / EXP(52 * LOG(2))-1023))),53,LEN(hex_Value)))) --- FLOAT
				WHEN system_type_id =59 THEN  Left(LTRIM(STR(CAST(SIGN(CAST(Convert(VARBINARY(8000),REVERSE(hex_Value)) AS BIGINT))* (1.0 + (CAST(CONVERT(VARBINARY(8000),Reverse(hex_Value)) AS BIGINT) & 0x007FFFFF) * POWER(CAST(2 AS Real), -23)) * POWER(CAST(2 AS Real),(((CAST(CONVERT(VARBINARY(8000),Reverse(hex_Value)) AS INT) )& 0x7f800000)/ EXP(23 * LOG(2))-127))AS REAL),23,23)),8) --Real
				WHEN system_type_id In (165,173) THEN (CASE WHEN CHARINDEX(0x,cast('' AS XML).value('xs:hexBinary(sql:column("hex_Value"))', 'VARBINARY(8000)')) = 0 THEN '0x' ELSE '' END) +cast('' AS XML).value('xs:hexBinary(sql:column("hex_Value"))', 'varchar(max)') -- BINARY,VARBINARY
				WHEN system_type_id =36 THEN CONVERT(VARCHAR(MAX),CONVERT(UNIQUEIDENTIFIER,hex_Value)) --UNIQUEIDENTIFIER
				END AS [FieldValue]
			,NULL AS [FieldSetValue]
			,[Rowlogcontents]
			,[TransactionID]
			,[Slot ID]
			,[ID]
			,[nullbit]
			,[TRANSACTION ID]
			,[Transaction Name]
			,[Datum]
			,[Current LSN]
			,[Begin Time]
		FROM CTE
		ORDER BY [nullbit]

	UPDATE #temp_Data SET [FieldSetValue] = --> New Column data
		(CASE
			-- VARCHAR; CHAR; 
			WHEN system_type_id In (167,175,189) THEN ISNULL('''' + [FieldValue] + '''','NULL')
			-- NVARCHAR; NCHAR
			WHEN system_type_id In (231,239) THEN ISNULL('N''' + [FieldValue] + '''','NULL')
			-- SMALLDATETIME; DATE; DATETIME; UNIQUEIDENTIFIER
			WHEN system_type_id In (58,40,61,36) THEN ISNULL('''' + [FieldValue] + '''','NULL')
			-- TINYINT; SMALLINT; INT; REAL; MONEY; FLOAT; BIT; DECIMAL; NUMERIC; SMALLMONEY; BIGINT
			WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN ISNULL([FieldValue],'NULL')
			END)
		FROM sys.columns [D]
		WHERE [object_id] = object_id(''+@SchemaName_n_TableName+'')
			AND [Fieldname] = D.[name]
 
/*Create Update statement*/
/*Now we have the modified and actual data as well*/
/*We need to create the update statement in case of recovery*/
 
;With CTE AS
	(SELECT
		QUOTENAME([Name]) + '=' + ISNULL([A].[FieldSetValue],'NULL') + ' , ' AS [Field]
		----(CASE
		----	-- VARCHAR; CHAR; 
		----	WHEN system_type_id In (167,175,189) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' ,' + ' '
		----	-- NVARCHAR; NCHAR
		----	WHEN system_type_id In (231,239) THEN QUOTENAME([Name]) + '=' + ISNULL('N''' + [A].[FieldValue] + '''','NULL') + ' ,' + ''
		----	-- SMALLDATETIME; DATE; DATETIME; UNIQUEIDENTIFIER
		----	WHEN system_type_id In (58,40,61,36) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' ,' + ' '
		----	-- TINYINT; SMALLINT; INT; REAL; MONEY; FLOAT; BIT; DECIMAL; NUMERIC; SMALLMONEY; BIGINT
		----	WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN QUOTENAME([Name]) + '=' + ISNULL([A].[FieldValue],'NULL') + ' ,' + ' '
		----	END) AS [Field]
		,A.[Slot ID]
		,A.[TransactionID] AS [TransactionID]
		,'D' AS [Type] --> Different
		,[A].[Rowlogcontents]
		,[A].[NonID]
		,[A].[nullbit]
		,[A].[TRANSACTION ID]
		,[A].[Transaction Name]
		,[A].[Datum]
		,[A].[Current LSN]
		,[A].[Begin Time]
	FROM #temp_Data AS [A]
		INNER JOIN #temp_Data AS [B] ON
			[A].[FieldName]=[B].[FieldName]
			AND [A].[Slot ID]=[B].[Slot ID]
			--And [A].[TransactionID]=[B].[TransactionID]+1
			AND [A].[TRANSACTION ID]=[B].[TRANSACTION ID] --> new condition
			AND [A].[TransactionID]<>[B].[TransactionID] --> new condition
			----AND [B].[TransactionID] =
			----	(SELECT Min(Cast([TransactionID] AS INT)) AS [TransactionID]
			----	FROM #temp_Data AS [C]
			----	WHERE [A].[Slot ID]=[C].[Slot ID]
			----	GROUP BY [Slot ID]
			----	)
		INNER JOIN sys.columns [D] ON [object_id] = object_id(''+@SchemaName_n_TableName+'')
			AND A.[Fieldname] = D.[name]
	WHERE ISNULL([A].[FieldValue],'') <> ISNULL([B].[FieldValue],'')

	UNION ALL
	 
	SELECT
		QUOTENAME([Name]) + '=' + ISNULL([A].[FieldSetValue],'NULL') + ' AND' AS [Field]
		----(CASE
		----	WHEN system_type_id In (167,175,189) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (231,239) THEN QUOTENAME([Name]) + '='+ ISNULL('N''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (58,40,61,36) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN QUOTENAME([Name]) + '=' + ISNULL([A].[FieldValue],'NULL') + ' AND'
		----	END) AS [Field]
		,A.[Slot ID]
		,A.[TransactionID] AS [TransactionID]
		,'S' AS [Type] --> Same
		,[A].[Rowlogcontents]
		,[A].[NonID]
		,[A].[nullbit]
		,[A].[TRANSACTION ID]
		,[A].[Transaction Name]
		,[A].[Datum]
		,[A].[Current LSN]
		,[A].[Begin Time]
	FROM #temp_Data AS [A]
		INNER JOIN #temp_Data AS [B] ON [A].[FieldName] = [B].[FieldName]
			AND [A].[Slot ID] = [B].[Slot ID]
			--AND [A].[TransactionID] = [B].[TransactionID]+1
			AND [A].[TRANSACTION ID]=[B].[TRANSACTION ID] --> new condition
			AND [A].[TransactionID]<>[B].[TransactionID] --> new condition
			----AND [B].[TransactionID] =
			----	(SELECT MIN(CAST([TransactionID] AS INT)) AS [TransactionID]
			----	FROM #temp_Data AS [C]
			----	WHERE [A].[Slot ID]=[C].[Slot ID]
			----	GROUP BY [Slot ID]
			----	)
		INNER JOIN sys.columns [D] ON [object_id] = object_id('' + @SchemaName_n_TableName + '')
			AND [A].[Fieldname] = D.[name]
	WHERE ISNULL([A].[FieldValue],'') = ISNULL([B].[FieldValue],'')
		----AND A.[TransactionID] NOT IN
		----	(SELECT MIN(CAST([TransactionID] AS INT)) AS [TransactionID]
		----	FROM #temp_Data AS [C]
		----	WHERE [A].[Slot ID]=[C].[Slot ID]
		----	GROUP BY [Slot ID]
		----	)

	UNION ALL --> new

	SELECT --> new
		QUOTENAME([Name]) + '=' + ISNULL([A].[FieldSetValue],'NULL') + ' AND' AS [Field]
		----(CASE
		----	WHEN system_type_id In (167,175,189) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (231,239) THEN QUOTENAME([Name]) + '=' + ISNULL('N''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (58,40,61,36) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN QUOTENAME([Name]) + '=' + ISNULL([A].[FieldValue],'NULL') + ' AND'
		----	END) AS [Field]
		,A.[Slot ID]
		,A.[TransactionID] AS [TransactionID]
		,CASE WHEN ([A].[Transaction Name] = 'INSERT') THEN 'N' ELSE 'O' END AS [Type] --> New or Old
		,[A].[Rowlogcontents]
		,[A].[NonID]
		,[A].[nullbit]
		,[A].[TRANSACTION ID]
		,[A].[Transaction Name]
		,[A].[Datum]
		,[A].[Current LSN]
		,[A].[Begin Time]
	FROM #temp_Data AS [A]
		INNER JOIN #temp_Data AS [B] ON 
			[A].[FieldName]=[B].[FieldName]
			AND [A].[Slot ID]=[B].[Slot ID]
			AND [A].[Transaction Name] IN ('INSERT','DELETE')
			AND [A].[TransactionID]=[B].[TransactionID] --> self
		INNER JOIN sys.columns [D] ON [object_id] = object_id(''+@SchemaName_n_TableName+'')
			AND A.[Fieldname] = D.[name]
	)
 
	,CTEUpdateQuery AS
		(SELECT 
			CASE A.[Transaction Name]
				WHEN 'UPDATE' THEN
					'UPDATE [' +  @SchemaName_n_TableName + '] SET ' +
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([Field],'')
							FROM CTE B 
							WHERE A.[Slot ID]=B.[Slot ID] AND A.[TransactionID]=B.[TransactionID] AND B.[Type]='D' FOR XML PATH('')
							),1,1,''), LEN(STUFF(
								(SELECT ' ' + ISNULL([Field],'')
								FROM CTE B 
								WHERE A.[Slot ID]=B.[Slot ID] AND A.[TransactionID]=B.[TransactionID] AND B.[Type]='D' FOR XML PATH('')
								),1,1,'') )-2 --> -2 removes the final ', '
						) +
 
						'  WHERE  ' +
 
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([Field],'') + ' '
							FROM CTE C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='S' FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' ' + ISNULL([Field],'') + ' '
								FROM CTE C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='S' FOR XML PATH('')
								),1,1,'') )-4 --> -4 removes the final ' AND'
						)
				WHEN 'DELETE' THEN
					'INSERT INTO [' +  @SchemaName_n_TableName + '] (' +
						LEFT(STUFF(
							(SELECT ' [' + ISNULL([FieldName] + '],','')
							FROM #temp_Data C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' [' + ISNULL([FieldName] + '],','')
								FROM #temp_Data C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
								),1,1,'') )-1 --> -1 removes the final ','
						) + ') VALUES (' +
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([FieldSetValue] + ',','')
							FROM #temp_Data C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' ' + ISNULL([FieldSetValue] + ',','')
								FROM #temp_Data C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
								),1,1,'') )-1 --> -1 removes the final ','
						) + ')'
				WHEN 'INSERT' THEN
					'DELETE FROM [' +  @SchemaName_n_TableName + '] WHERE ' +
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([Field],'')
							FROM CTE C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='N' FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' ' + ISNULL([Field],'')
								FROM CTE C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='N' FOR XML PATH('')
								),1,1,'') )-4 --> -4 removes the final ' AND'
						)
				END AS [Update Statement]
			,[Slot ID]
			,[TransactionID]
			,[Rowlogcontents]
			,[A].[NonID]
			,MAX([A].[nullbit])+1 AS [nullbit]
			,[A].[TRANSACTION ID]
			,[A].[Transaction Name]
			,[A].[Datum]
			,[A].[Current LSN]
			,[A].[Begin Time]
		FROM CTE A
		GROUP BY [Slot ID]
			,[TransactionID]
			,[Rowlogcontents]
			,[A].[NonID]
			,[A].[TRANSACTION ID]
			,[A].[Transaction Name]
			,[A].[Begin Time]
			,[A].[Current LSN]
			,[A].[Datum]
		)

	INSERT INTO #temp_Data 
		SELECT 'Update Statement',ISNULL([Update Statement],''),NULL,NULL,[TransactionID],[Slot ID],[NonID],[nullbit],[TRANSACTION ID],[Transaction Name],[Datum],[Current LSN],[Begin Time]
		FROM CTEUpdateQuery

/****************************************/
IF (@Debug > 0) BEGIN
	print 'select * from CTE1'
	select 'CTE1'
	select * from (
	/*This part is for variable data columns*/
	SELECT
		 A.[ID]
		,[Rowlogcontents]
		,[TransactionID]
		,[Slot ID]
		,[TRANSACTION ID]
		,[Transaction Name]
		,[Datum]
		,[Current LSN]
		,[Begin Time]
		,[NAME]
		,cols.leaf_null_bit AS nullbit
		,leaf_offset
		,ISNULL(syscolumns.length, cols.max_length) AS [length]
		,cols.system_type_id
		,cols.leaf_bit_position AS bitpos
		,ISNULL(syscolumns.xprec, cols.precision) AS xprec
		,ISNULL(syscolumns.xscale, cols.scale) AS xscale
		,SUBSTRING([nullBitMap], cols.leaf_null_bit, 1) AS is_null
		--Calculate the variable column size from the variable column offset array
		,(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
			THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
			ELSE 0 END
		) AS [Column value Size]
 
		--Calculate the column length
		,(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
			THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING ([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
				- ISNULL(NULLIF(CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE (SUBSTRING([ColumnOffsetArray], (2*((leaf_offset*-1)-1))-1, 2)))), 0), [varColumnStart]) --> If Null OR zero
			ELSE 0 END
		) AS [Column Length]
 
		--Get the Hexa decimal value from the RowlogContent
		--HexValue of the variable column=Substring([Column value Size] - [Column Length] + 1,[Column Length])
		--This is the data of your column but in the Hexvalue
		,CASE WHEN SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=1
			THEN NULL
			ELSE SUBSTRING(Rowlogcontents,
				(
				(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
					THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
					ELSE 0 END
				)
				-(
				(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
					THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
						- ISNULL(NULLIF(CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*((leaf_offset*-1)-1))-1, 2)))), 0), [varColumnStart]) --> If Null OR zero
					ELSE 0 END
				))
				)+1,(
				(CASE WHEN leaf_offset<1 AND SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=0
					THEN CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*(leaf_offset*-1))-1, 2))))
						- ISNULL(NULLIF(CONVERT(SMALLINT, CONVERT(BINARY(2), REVERSE(SUBSTRING([ColumnOffsetArray], (2*((leaf_offset*-1)-1))-1, 2)))), 0), [varColumnStart]) --> If Null OR zero
					ELSE 0 END
				))
				)
			END AS hex_Value
 
	FROM @DeletedRecords A
		INNER JOIN sys.allocation_units allocunits ON
			A.[AllocUnitId]=allocunits.[Allocation_Unit_Id]
		INNER JOIN sys.partitions partitions ON
			(allocunits.type IN (1, 3) AND partitions.hobt_id = allocunits.container_id)
			OR (allocunits.type = 2 AND partitions.partition_id = allocunits.container_id)
		INNER JOIN sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id
		LEFT OUTER JOIN syscolumns ON syscolumns.id = partitions.object_id AND syscolumns.colid = cols.partition_column_id
	WHERE leaf_offset < 0

	UNION
	/*This part is for fixed data columns*/
	SELECT 
		 A.[ID]
		,[Rowlogcontents]
		,[TransactionID]
		,[Slot ID]
		,[TRANSACTION ID]
		,[Transaction Name]
		,[Datum]
		,[Current LSN]
		,[Begin Time]
		,[NAME]
		,cols.leaf_null_bit AS [nullbit]
		,leaf_offset
		,ISNULL(syscolumns.length, cols.max_length) AS [length]
		,cols.system_type_id
		,cols.leaf_bit_position AS bitpos
		,ISNULL(syscolumns.xprec, cols.precision) AS xprec
		,ISNULL(syscolumns.xscale, cols.scale) AS xscale
		,SUBSTRING([nullBitMap], cols.leaf_null_bit, 1) AS is_null
		,(SELECT TOP 1 ISNULL(SUM(CASE WHEN C.leaf_offset >1 THEN max_length ELSE 0 END),0)
		FROM sys.system_internals_partition_columns C
		WHERE cols.partition_id=C.partition_id AND C.leaf_null_bit<cols.leaf_null_bit
		)+5 AS [Column value Size]
		,syscolumns.length AS [Column Length]
 
		,CASE WHEN SUBSTRING([nullBitMap], cols.leaf_null_bit, 1)=1
			THEN NULL
			ELSE SUBSTRING(Rowlogcontents,
				(SELECT TOP 1 ISNULL(SUM(CASE WHEN C.leaf_offset >1 THEN max_length ELSE 0 END),0)
				FROM sys.system_internals_partition_columns C
				WHERE cols.partition_id =C.partition_id AND C.leaf_null_bit<cols.leaf_null_bit
				)+5
				,syscolumns.length)
			END AS hex_Value
	FROM @DeletedRecords A
		INNER JOIN sys.allocation_units allocunits ON A.[AllocUnitId]=allocunits.[Allocation_Unit_Id]
		INNER JOIN sys.partitions partitions ON
			(allocunits.type IN (1, 3) AND partitions.hobt_id = allocunits.container_id)
			OR (allocunits.type = 2 AND partitions.partition_id = allocunits.container_id)
		INNER JOIN sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id
		LEFT OUTER JOIN syscolumns ON syscolumns.id = partitions.object_id AND syscolumns.colid = cols.partition_column_id
	WHERE leaf_offset>0
	) AS t1
	order by [Begin Time],[ID],[nullbit]

	print 'select * from CTE2'
	select 'CTE2'
	select * into #CTE from (
	SELECT
		QUOTENAME([Name]) + '=' + ISNULL([A].[FieldSetValue],'NULL') + ' , ' AS [Field]
		----(CASE
		----	-- VARCHAR; CHAR; 
		----	WHEN system_type_id In (167,175,189) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' ,' + ' '
		----	-- NVARCHAR; NCHAR
		----	WHEN system_type_id In (231,239) THEN QUOTENAME([Name]) + '=' + ISNULL('N''' + [A].[FieldValue] + '''','NULL') + ' ,' + ''
		----	-- SMALLDATETIME; DATE; DATETIME; UNIQUEIDENTIFIER
		----	WHEN system_type_id In (58,40,61,36) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' ,' + ' '
		----	-- TINYINT; SMALLINT; INT; REAL; MONEY; FLOAT; BIT; DECIMAL; NUMERIC; SMALLMONEY; BIGINT
		----	WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN QUOTENAME([Name]) + '=' + ISNULL([A].[FieldValue],'NULL') + ' ,' + ' '
		----	END) AS [Field]
		,A.[Slot ID]
		,A.[TransactionID] AS [TransactionID]
		,'D' AS [Type] --> Different
		,[A].[Rowlogcontents]
		,[A].[NonID]
		,[A].[nullbit]
		,[A].[TRANSACTION ID]
		,[A].[Transaction Name]
		,[A].[Datum]
		,[A].[Current LSN]
		,[A].[Begin Time]
	FROM #temp_Data AS [A]
		INNER JOIN #temp_Data AS [B] ON 
			[A].[FieldName]=[B].[FieldName]
			AND [A].[Slot ID]=[B].[Slot ID]
			--And [A].[TransactionID]=[B].[TransactionID]+1
			AND [A].[TRANSACTION ID]=[B].[TRANSACTION ID] --> new condition
			AND [A].[TransactionID]<>[B].[TransactionID] --> new condition
			AND [A].[Current LSN]=[B].[Current LSN] --> new condition
			----AND [B].[TransactionID] =
			----	(SELECT Min(Cast([TransactionID] AS INT)) AS [TransactionID]
			----	FROM #temp_Data AS [C]
			----	WHERE [A].[Slot ID]=[C].[Slot ID]
			----	GROUP BY [Slot ID]
			----	)
		INNER JOIN sys.columns [D] ON [object_id] = object_id(''+@SchemaName_n_TableName+'')
			AND A.[Fieldname] = D.[name]
	WHERE ISNULL([A].[FieldValue],'') <> ISNULL([B].[FieldValue],'')

	UNION ALL
	 
	SELECT
		QUOTENAME([Name]) + '=' + ISNULL([A].[FieldSetValue],'NULL') + ' AND' AS [Field]
		----(CASE
		----	WHEN system_type_id In (167,175,189) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (231,239) THEN QUOTENAME([Name]) + '='+ ISNULL('N''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (58,40,61,36) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN QUOTENAME([Name]) + '=' + ISNULL([A].[FieldValue],'NULL') + ' AND'
		----	END) AS [Field]
		,A.[Slot ID]
		,A.[TransactionID] AS [TransactionID]
		,'S' AS [Type] --> Same
		,[A].[Rowlogcontents]
		,[A].[NonID]
		,[A].[nullbit]
		,[A].[TRANSACTION ID]
		,[A].[Transaction Name]
		,[A].[Datum]
		,[A].[Current LSN]
		,[A].[Begin Time]
	FROM #temp_Data AS [A]
		INNER JOIN #temp_Data AS [B] ON [A].[FieldName] = [B].[FieldName]
			AND [A].[Slot ID] = [B].[Slot ID]
			--AND [A].[TransactionID] = [B].[TransactionID]+1
			AND [A].[TRANSACTION ID]=[B].[TRANSACTION ID] --> new condition
			AND [A].[TransactionID]<>[B].[TransactionID] --> new condition
			AND [A].[Current LSN]=[B].[Current LSN] --> new condition
			----AND [B].[TransactionID] =
			----	(SELECT MIN(CAST([TransactionID] AS INT)) AS [TransactionID]
			----	FROM #temp_Data AS [C]
			----	WHERE [A].[Slot ID]=[C].[Slot ID]
			----	GROUP BY [Slot ID]
			----	)
		INNER JOIN sys.columns [D] ON [object_id] = object_id('' + @SchemaName_n_TableName + '')
			AND [A].[Fieldname] = D.[name]
	WHERE ISNULL([A].[FieldValue],'') = ISNULL([B].[FieldValue],'')
		----AND A.[TransactionID] NOT IN
		----	(SELECT MIN(CAST([TransactionID] AS INT)) AS [TransactionID]
		----	FROM #temp_Data AS [C]
		----	WHERE [A].[Slot ID]=[C].[Slot ID]
		----	GROUP BY [Slot ID]
		----	)

	UNION ALL --> new

	SELECT --> new
		QUOTENAME([Name]) + '=' + ISNULL([A].[FieldSetValue],'NULL') + ' AND' AS [Field]
		----(CASE
		----	WHEN system_type_id In (167,175,189) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (231,239) THEN QUOTENAME([Name]) + '=' + ISNULL('N''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (58,40,61,36) THEN QUOTENAME([Name]) + '=' + ISNULL('''' + [A].[FieldValue] + '''','NULL') + ' AND'
		----	WHEN system_type_id In (48,52,56,59,60,62,104,106,108,122,127) THEN QUOTENAME([Name]) + '=' + ISNULL([A].[FieldValue],'NULL') + ' AND'
		----	END) AS [Field]
		,A.[Slot ID]
		,A.[TransactionID] AS [TransactionID]
		,CASE WHEN ([A].[Transaction Name] = 'INSERT') THEN 'N' ELSE 'O' END AS [Type] --> New or Old
		,[A].[Rowlogcontents]
		,[A].[NonID]
		,[A].[nullbit]
		,[A].[TRANSACTION ID]
		,[A].[Transaction Name]
		,[A].[Datum]
		,[A].[Current LSN]
		,[A].[Begin Time]
	FROM #temp_Data AS [A]
		INNER JOIN #temp_Data AS [B] ON 
			[A].[FieldName]=[B].[FieldName]
			AND [A].[Slot ID]=[B].[Slot ID]
			AND [A].[Transaction Name] IN ('INSERT','DELETE')
			AND [A].[TransactionID]=[B].[TransactionID] --> self
		INNER JOIN sys.columns [D] ON [object_id] = object_id(''+@SchemaName_n_TableName+'')
			AND A.[Fieldname] = D.[name]
	) as t2
	select * from #CTE
	order by [Begin Time],[Slot ID],[NonID],[nullbit]

	print 'select * from CTEUpdateQuery'
	select 'CTEUpdateQuery'
		SELECT 
			CASE A.[Transaction Name]
				WHEN 'UPDATE' THEN
					'UPDATE [' +  @SchemaName_n_TableName + '] SET ' +
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([Field],'')
							FROM #CTE B 
							WHERE A.[Slot ID]=B.[Slot ID] AND A.[TransactionID]=B.[TransactionID] AND B.[Type]='D' FOR XML PATH('')
							),1,1,''), LEN(STUFF(
								(SELECT ' ' + ISNULL([Field],'')
								FROM #CTE B 
								WHERE A.[Slot ID]=B.[Slot ID] AND A.[TransactionID]=B.[TransactionID] AND B.[Type]='D' FOR XML PATH('')
								),1,1,'') )-2 --> -2 removes the final ', '
						) +
 
						'  WHERE  ' +
 
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([Field],'') + ' '
							FROM #CTE C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='S' FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' ' + ISNULL([Field],'') + ' '
								FROM #CTE C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='S' FOR XML PATH('')
								),1,1,'') )-4 --> -4 removes the final ' AND'
						)
				WHEN 'DELETE' THEN
					'INSERT INTO [' +  @SchemaName_n_TableName + '] (' +
						LEFT(STUFF(
							(SELECT ' [' + ISNULL([FieldName] + '],','')
							FROM #temp_Data C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' [' + ISNULL([FieldName] + '],','')
								FROM #temp_Data C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
								),1,1,'') )-1 --> -1 removes the final ','
						) + ') VALUES (' +
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([FieldSetValue] + ',','')
							FROM #temp_Data C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' ' + ISNULL([FieldSetValue] + ',','')
								FROM #temp_Data C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Rowlogcontents] IS NOT NULL FOR XML PATH('')
								),1,1,'') )-1 --> -1 removes the final ','
						) + ')'
				WHEN 'INSERT' THEN
					'DELETE FROM [' +  @SchemaName_n_TableName + '] WHERE ' +
						LEFT(STUFF(
							(SELECT ' ' + ISNULL([Field],'')
							FROM #CTE C 
							WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='N' FOR XML PATH('')
							),1,1,'') ,LEN(STUFF(
								(SELECT ' ' + ISNULL([Field],'')
								FROM #CTE C 
								WHERE A.[Slot ID]=C.[Slot ID] AND A.[TransactionID]=C.[TransactionID] AND C.[Type]='N' FOR XML PATH('')
								),1,1,'') )-4 --> -4 removes the final ' AND'
						)
				END AS [Update Statement]
			,[Slot ID]
			,[TransactionID]
			,[Rowlogcontents]
			,[A].[NonID]
			,MAX([A].[nullbit])+1 AS [nullbit]
			,[A].[TRANSACTION ID]
			,[A].[Transaction Name]
			,[A].[Datum]
			,[A].[Current LSN]
			,[A].[Begin Time]
		FROM #CTE A
		GROUP BY [Slot ID]
			,[TransactionID]
			,[Rowlogcontents]
			,[A].[NonID]
			,[A].[TRANSACTION ID]
			,[A].[Transaction Name]
			,[A].[Begin Time]
			,[A].[Current LSN]
			,[A].[Datum]
	drop table #CTE
END

IF (@Debug > 0) BEGIN
	print 'select * from temp_Data1'
	select '#temp_Data1'
	select * from #temp_Data order by [TransactionID],[nullbit]
END

--Create the column name in the same order to do pivot table.
DECLARE @FieldNames VARCHAR(MAX)
SET @FieldNames = STUFF(
	(SELECT ','+CAST(QUOTENAME([Name]) AS VARCHAR(MAX))
	FROM syscolumns
	WHERE id = object_id(''+@SchemaName_n_TableName+'') FOR XML PATH('')
	),1,1,'')

--Finally, pivot table and get the change history so one can get the data back.
--The [Update Statement] column will give you the query that you can execute for recovery.
-- NOTE: Need a placeholder type for [Execution Account] (else it will be int)
SET @sql = 'SELECT ROW_NUMBER() OVER (ORDER BY [LSN],[Datum] DESC) AS [RowNo],' + @FieldNames
		+ ',[Datum],[Update Statement],[Transaction],-1 AS [Prev RowNo],[Date],[Execution Account],[TRANSACTION ID],[Slot ID],[LSN] '
	+ 'INTO [@ResultTable] '
	+ 'FROM (SELECT [FieldName],[FieldValue],[Transaction Name] AS [Transaction],[Datum] AS [Datum],[Begin Time] AS [Date]'
		+',[TRANSACTION ID] AS [Execution Account],[TRANSACTION ID],[Slot ID],[Current LSN] AS [LSN] FROM #temp_Data) AS src '
	+ 'PIVOT (Min([FieldValue]) FOR [FieldName] IN (' + @FieldNames  + ',[Update Statement])) AS pvt '
	+ 'ORDER BY [LSN],[Datum] DESC'

--> Create a table to hold the dynamic SQL results --> doesn't seem to work
DECLARE @sql1 VARCHAR(MAX)
SET @sql1 = 'Declare @result Table ([RecNo] INT NOT NULL IDENTITY(1,1) PRIMARY KEY,' + REPLACE(@FieldNames,',',' varchar(max),') + 'varchar(max),'
	+ '[Datum] varchar(20),[Update Statement] varchar(max),[TRANSACTION ID] varchar(max),[Transaction] varchar(max)'
	+',[Slot ID] int,[LSN] varchar(max),[Prev RowNo] int,[Date] varchar(20))'
--EXEC(@sql1)

EXEC sp_executesql @sql
IF OBJECT_ID (N'[@ResultTable]', N'U') IS NOT NULL 
BEGIN
	UPDATE [@ResultTable] SET [Prev RowNo] =
		ISNULL((SELECT MAX(B.[RowNo]) AS [ID] FROM [@ResultTable] B
				WHERE B.[Slot ID] = A.[Slot ID] AND B.[RowNo] < A.[RowNo]),0)
		FROM [@ResultTable] A
	UPDATE [@ResultTable] SET [Execution Account] =
		(SELECT [Transaction Account] FROM #TransIdAllList B WHERE B.[Transaction ID] = A.[Transaction ID])
		FROM [@ResultTable] A
	SELECT * FROM [@ResultTable]
	DROP TABLE [@ResultTable]
END

IF (@Debug > 0) BEGIN
	print @FieldNames
	print @sql1
	print ''
	print @sql
END

GO --> required for testing/debugging (dumps all local entities from above)
SET NOEXEC OFF




--> SQL 2005; PIVOT requires compatibility_level = 90
DECLARE @Database_Name NVARCHAR(MAX)
SET @Database_Name = 'Test'
--SELECT [compatibility_level] from sys.databases where [name] = @Database_Name
IF ((SELECT [compatibility_level] from sys.databases where [name] = @Database_Name) < 90)
	EXEC sp_dbcmptlevel @Database_Name, 90

--Execute the procedure like
--Recover_Modified_Data_Proc 'Database name''Schema.table name','Date from' ,'Date to'

----EXAMPLE #1 : FOR ALL MODIFIED RECORDS
--EXEC Recover_Modified_Data_Proc @Database_Name,'dbo.Student'

--EXAMPLE #2 : FOR ANY SPECIFIC DATE RANGE
EXEC Recover_Modified_Data_Proc  @Database_Name,'dbo.Student','2014/05/22','2099/01/01'
--It will give you the result of all modified records.

--> Example
IF (1 = 0)
BEGIN --> Execute example table code by selection
	DROP TABLE [dbo].[Student]
	CREATE TABLE [dbo].[Student](
		  [Sno] [int] NOT NULL,
		  [Student ID] nvarchar(6) Not NULL ,
		  [Student name] [varchar](50) NOT NULL,
		  [Date of Birth]  datetime not null,
		  [Weight] [int] NULL)
	--Inserting data into table
	Insert into dbo.[Student] values (1,'STD001','Bob','2003-12-31',40)
	Insert into dbo.[Student] values (2,'STD002','Alexander','2004-11-15',35)
	Insert into dbo.[Student] values (3,'STD003','Phil','2005-10-09',27)
	Insert into dbo.[Student] values (4,'STD004','Paul','2006-10-09',31)
	Insert into dbo.[Student] values (5,'STD005','Steve','2007-10-09',32)
	Insert into dbo.[Student] values (6,'STD006','John','2008-10-09',33)
	--Check the existence of the data
	Select * from dbo.[Student]

	Delete From [Student] Where [Student ID]='STD002'
	--By mistake if all records are updated instead of one record
	Update [Student] Set [Student Name]='Bob jerry' --> Operation type will be 'LOP_MODIFY_ROW'
	Update [Student] Set [Student Name]='Bob greg' Where [SNO]=3 --> Operation type will be 'LOP_MODIFY_ROW' alone or 'LOP_MODIFY_COLUMNS' with previous update
	Update [Student] Set [Student Name]='Bob george' Where [SNO]=3
	Update [Student] Set [Student Name]='Jim' Where [SNO]=3
	Update [Student] Set [Student Name]='Ben',[Weight]=25,[Date of Birth]='2003-12-30' Where [SNO]=1 --> Operation type will be 'LOP_MODIFY_COLUMNS'
	Update [Student] Set [Student Name]='James',[Weight]=55,[Date of Birth]='2006-10-08' Where [SNO]=4 --> Operation type will be 'LOP_MODIFY_COLUMNS'
	Delete From [Student] Where [Student ID]='STD003'
	Delete From [Student] Where [Student ID]IN('STD001','STD004')
	--Verify the data has been modified
	Select * from dbo.[Student]
END

/*****************************************
system_type_id vs. type name
(single byte ASCII)
167    VARCHAR
175    CHAR
189    TIMESTAMP

(2-byte Unicode)
231    NVARCHAR
239    NCHAR

58     SMALLDATETIME
40     DATE (2008+)
61     DATETIME
36     UNIQUEIDENTIFIER

(numeric data)
48     TINYINT
52     SMALLINT
56     INT (or INTEGER)
59     REAL
60     MONEY
62     FLOAT
104    BIT
106    DECIMAL
108    NUMERIC
122    SMALLMONEY
127    BIGINT

(not supported)
165    BINARY
173    VARBINARY

*****************************************
system_type_id    name
---------------- -------------------
        34         image
        35         text
        36         uniqueidentifier
        40         date
        41         time
        42         datetime2
        43         datetimeoffset
        48         tinyint
        52         smallint
        56         int
        58         smalldatetime
        59         real
        60         money
        61         datetime
        62         float
        98         sql_variant
        99         ntext
       104         bit
       106         decimal
       108         numeric
       122         smallmoney
       127         bigint
       165         varbinary
       167         varchar
       173         binary
       175         char
       189         timestamp
       231         nvarchar
       231         sysname
       239         nchar
       240         hierarchyid
       240         geometry
       240         geography
       241         xml
*****************************************/
