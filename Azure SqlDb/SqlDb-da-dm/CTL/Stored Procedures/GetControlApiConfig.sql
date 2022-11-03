CREATE PROCEDURE [CTL].[GetControlApiConfig] (@SourceId BigInt)
AS
BEGIN
     
  Declare @TableName VARCHAR(250);
  SELECT @TableName = Replace(SourceName, '/', '_') FROM CTL.ControlSource Where SourceId = @SourceId;

  DECLARE @TrustedTabCountSQL NVARCHAR(4000);
  DECLARE @TrustedTabCount BIGINT;

  IF OBJECT_ID('Trusted.' + @TableName) IS NOT NULL
  BEGIN
     SET @TrustedTabCountSQL = 'Select Top 1 @TrustedTabCount = count(*) FROM Trusted.' +@TableName;
     EXEC sp_executesql @StagingTabCountSQL, N'@TrustedTabCount BIGINT out', @TrustedTabCount out
  END
  ELSE
  BEGIN
     SET @TrustedTabCount = 0;
  END


  Declare @Get_Load_Date_SQL NVARCHAR(4000);
  Declare @Load_Date VARCHAR(100);

  IF OBJECT_ID('Trusted.' + @TableName) IS NOT NULL AND @TrustedTabCount > 0
    BEGIN
      SET @Get_Load_Date_SQL = 'Select @Load_Date = Convert(Datetime2, Convert(Date, WaterMarks)) FROM CTL.ControlWaterMark Where ControlSourceId = ' +@SourceId
    END
  ELSE
    BEGIN
      SET @Get_Load_Date_SQL = ''2000-01-01T00:00:42Z''
    END  

  EXEC sp_executesql @Get_Load_Date_SQL, N'@Load_Date datetime2 out', @Load_Date out  

  Select ApiConfig.*, @Load_Date as if_modified_since
    From CTL.ControlApiConfig ApiConfig
         Left Join CTL.ControlSource src
                on src.SourceId = ApiConfig.SourceId
   Where 1=1
     and ApiConfig.SourceId = @SourceId  

END
;

GO


