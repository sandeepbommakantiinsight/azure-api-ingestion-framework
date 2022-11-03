CREATE TABLE [CTL].[ControlApiConfig](
	[ApiConfigId] [bigint] IDENTITY(1,1) NOT NULL,
	[SourceId] [bigint] NOT NULL,
	[SourceApp] [varchar](100) NULL,
	[SourceEndPoint] [varchar](100) NULL,
	[SourceTableName] [varchar](100) NULL,
	[SourceTableJSONStructure] [nvarchar](max) NULL,
	[SourceTableQuery] [nvarchar](max) NULL,
	[SourceConfig] [varchar](100) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


