-- �ncelikle Schema lar� Create ediyoruz. --veritaban� i�inde tablolar�, prosed�rleri gruplamak i�in kullan�lan mant�ksal bir klas�r.

CREATE SCHEMA fwk;
GO

CREATE SCHEMA ext;
GO

CREATE SCHEMA stg;
GO

-- Kullan�m s�ras�na g�re SP leri  Create ediyoruz.-----
--* 1. SP

USE [DB_Name]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [fwk].[sp_InsertTableDefinition]
    @source_system_id tinyint,
    @etl_method_id tinyint,
    @source_object_name nvarchar(100),
    @schema_name nvarchar(5),
    @table_name nvarchar(100),
    @full_table_name nvarchar(100),
    @table_description nvarchar(200),
    @unique_columns nvarchar(100),
    @etl_reference_columns nvarchar(100)
AS
BEGIN
    INSERT INTO [fwk].[TableDefinition] (
        [SourceSystemId],
        [EtlMethodId],
        [SourceObjectName],
        [SchemaName],
        [TableName],
        [FullTableName],
        [TableDescription],
        [UniqueColumns],
        [EtlReferenceColumns]
    )
    VALUES (
        @source_system_id,
        @etl_method_id,
        @source_object_name,
        @schema_name,
        @table_name,
        @full_table_name,
        @table_description,
        @unique_columns,
        @etl_reference_columns
    );

    INSERT INTO fwk.ExecutionLog(TableId)
    SELECT TableId
    FROM fwk.TableDefinition
    WHERE FullTableName = @full_table_name;
END

--*2.SP - Veri al�m� i�lemi i�in fwk.ExecutionLog tablosuna EtlStartDate = GETDATE(),LastStatus = 'InProgress' olan bir kay�t at�yor.

USE [DB_Name]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


Create PROC [fwk].[sp_EtlStartingCommand]

@p_table_id int,
@p_package_name nvarchar(100),
@p_load_type nvarchar(50)

AS

SELECT ISNULL(EtlSuccessfulStartDate,'2000-01-01') AS EtlSuccessfulStartDate
FROM fwk.ExecutionLog (NOLOCK)
WHERE TableId = @p_table_id 

UPDATE fwk.ExecutionLog
SET 
	EtlStartDate = GETDATE()
	,EtlEndDate = NULL
	,LastStatus = 'InProgress'
	,PackageName = @p_package_name
	,LoadType = @p_load_type

WHERE TableId = @p_table_id 

--* 3. SP  StoredProcedure [fwk].[sp_TruncateTable] -- etl_method = 1 trancate �nsert yada load type initial load ise table definitiondan fulltablename ini alarak  trancate scrptini olu�tur ve �al��t�r. Parametrik olmas� SSIS taraf�nda i�imizi kolayla�t�r�yor. 

USE [DB_Name]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create PROC [fwk].[sp_TruncateTable] @p_table_id int,@p_load_type nvarchar(50)

AS

--DECLARE @p_table_id int = 6

DECLARE @truncate_string nvarchar(100)
DECLARE @etl_method_id int = (SELECT EtlMethodId FROM fwk.TableDefinition WHERE TableId = @p_table_id)

IF (@etl_method_id = 1) -- Truncate-Insert
	OR (@etl_method_id in (2) AND @p_load_type = 'Initial Load') -- Incremental Load - Initial Load

BEGIN

SET @truncate_string = 
'TRUNCATE TABLE ' +
(SELECT FullTableName FROM fwk.TableDefinition WHERE TableId=@p_table_id)

END

IF @etl_method_id IN (2) AND @p_load_type = 'Daily Refresh' -- Incremental Load

BEGIN

SET @truncate_string = 
'TRUNCATE TABLE ext.' +
(SELECT TableName FROM fwk.TableDefinition WHERE TableId=@p_table_id)

END

EXEC (@truncate_string)

--** 4. SP [fwk].[sp_EtlSuccessfulEndingCommand] -------
USE [DB_Name]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create PROC [fwk].[sp_EtlSuccessfulEndingCommand]
@p_table_id int,
@p_row_count int
AS
BEGIN
UPDATE fwk.ExecutionLog 
SET EtlSuccessfulStartDate = EtlStartDate, 
LastStatus = 'Successful' ,
EtlEndDate = getdate(),
ExtractRowCount = @p_row_count
WHERE TableId = @p_table_id

INSERT INTO fwk.ExecutionLogHistory
SELECT TableId,EtlStartDate,EtlSuccessfulStartDate,EtlEndDate,LoadType,LastStatus,PackageName,ExtractRowCount
FROM fwk.ExecutionLog (NOLOCK)
WHERE TableId = @p_table_id

END


--********* TABLOLARIN CREATE ED�LMES�*************------
--------- [fwk].[ExecutionLog] Log Tablosunu Create ediyoruz. Log kay�tlar� burada tutulacak. Son g�ncel �al��ma durumu olacak. Ge�mi� kay�tlar i�in history tablosu olacak.

USE [DB_Name]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [fwk].[ExecutionLog](
	[TableId] [int] NOT NULL,
	[EtlStartDate] [datetime] NULL,
	[EtlSuccessfulStartDate] [datetime] NULL,
	[EtlEndDate] [datetime] NULL,
	[LoadType] [nvarchar](50) NULL,
	[LastStatus] [nvarchar](50) NULL,
	[PackageName] [nvarchar](100) NULL,
	[ExtractRowCount] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[TableId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [fwk].[ExecutionLog] ADD  DEFAULT ('NeverStarted') FOR [LastStatus]
GO

ALTER TABLE [fwk].[ExecutionLog]  WITH CHECK ADD CHECK  (([LastStatus]='NeverStarted' OR [LastStatus]='Failed' OR [LastStatus]='InProgress' OR [LastStatus]='Successful'))
GO

---------- [fwk].[TableDefinition] Tablosunun Create edilmesi.
USE [DB_Name]
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [fwk].[TableDefinition](
	[TableId] [int] IDENTITY(1,1) NOT NULL,
	[SourceSystemId] [tinyint] NULL,
	[EtlMethodId] [tinyint] NULL,
	[SourceObjectName] [nvarchar](100) NULL,
	[SchemaName] [nvarchar](5) NULL,
	[TableName] [nvarchar](100) NULL,
	[FullTableName] [nvarchar](100) NULL,
	[TableDescription] [nvarchar](200) NULL,
	[UniqueColumns] [nvarchar](100) NULL,
	[EtlReferenceColumns] [nvarchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[TableId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [unq_fwk_table_definition_full_table_name] UNIQUE NONCLUSTERED 
(
	[FullTableName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-------------**************** Table [fwk].[EtlMethod] tablosunun Create edilmesi---------
USE [DB_Name]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [fwk].[EtlMethod](
	[EtlMethodId] [tinyint] IDENTITY(1,1) NOT NULL,
	[EtlMethodName] [nvarchar](50) NULL,
	[EtlMethodDescription] [nvarchar](200) NULL,
PRIMARY KEY CLUSTERED 
(
	[EtlMethodId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

------------------------------------***************
INSERT INTO [fwk].[EtlMethod] (EtlMethodName, EtlMethodDescription)
VALUES 
    (N'Truncate-Insert', NULL),
    (N'Incremental Load', NULL);

--------------------********************* Table [fwk].[ExecutionLogHistory] 
USE [DB_Name]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [fwk].[ExecutionLogHistory](
	[TableId] [int] NULL,
	[EtlStartDate] [datetime] NULL,
	[EtlSuccessfulStartDate] [datetime] NULL,
	[EtlEndDate] [datetime] NULL,
	[LoadType] [nvarchar](50) NULL,
	[LastStatus] [nvarchar](50) NULL,
	[PackageName] [nvarchar](100) NULL,
	[ExtractRowCount] [int] NULL
) ON [PRIMARY]
GO


-------------------------------------*********************

--********** herhangi bir  yeni tabloda aktaraca��m�z  zaman bu sp �al��t�r�l�yor ve sp_InsertTableDefinition a bu tablonun bilgileri yaz�l�yor.*********
 --bu SP yi SQL de �al��t�r�yoruz di�erleri SSIS �zerinden �al��acak 
EXEC [fwk].sp_InsertTableDefinition
	@source_system_id = 1, -- Verinin kaynak sistem id'sidir.
    @etl_method_id = 1, -- ETL methodun ID si dir. [EtlMethod] tablosunda detaylar� olacak 1 trancate-�nsert demek.
    @source_object_name = 'cdWarehouseDesc', -- Veri Kaynag�ndaki Tablo Giriliyor.
    @schema_name = 'stg', -- Veri Ambar�ndaki semay� belirtir
    @table_name = 'V3_cdWarehouseDesc', -- Veri Ambar�ndaki (Hedef) tablo ad� girilir
    @full_table_name ='stg.V3_cdWarehouseDesc', --Sema ve tablo ad� birlesimidir.
    @table_description ='', -- Tablo Ac�klamas�d�r.
    @unique_columns ='WarehouseCode,LangCode', -- Tablodaki sat�rlar�n essizligini belirleyen s�tunlar.(Uniquelik sa�laan s�tunlar)
    @etl_reference_columns = '' -- Incremental Refresh ETL methodu i�in gerekli s�tunlar� belirtir.(Create date, lastupdate date vb.)


select * from fwk.TableDefinition
select * from fwk.ExecutionLog
select * from fwk.EtlMethod
