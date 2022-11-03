CREATE PROC [CTL].[sp_add_api_config] @SourceApp [VARCHAR](100), @SourceEndPoint [VARCHAR](100), @SourceTableName [VARCHAR](500), @SourceTableJSONStructure NVARCHAR(MAX), @SourceTableQuery NVARCHAR(MAX) ,@SourceConfig [VARCHAR](100)
AS 
BEGIN

     DECLARE @SourceTypeId VARCHAR(100);
     Select @SourceTypeId = TypeId from CTL.ControlTypes Where ControlType = 'API'

     DECLARE @SourceID VARCHAR(100);
     Select @SourceID = SourceID from CTL.ControlSource Where SourceName = CONCAT(@SourceApp, '/', @SourceTableName) and SourceTypeId = @SourceTypeId

     INSERT INTO [CTL].[ControlApiConfig]
           ([SourceId]
           ,[SourceApp]    
           ,[SourceEndPoint]
           ,[SourceTableName]
           ,[SourceTableJSONStructure]
           ,[SourceTableQuery]
           ,[SourceConfig])
     VALUES
           (
               @SourceID
             , @SourceApp   
             , @SourceEndPoint 
             , @SourceTableName 
             , @SourceTableJSONStructure 
             , @SourceTableQuery 
             , @SourceConfig 
           )
     ;      

END

--exec sp_add_api_config @SourceApp='Xero', @SourceEndPoint='Accounts', @SourceTableName='Accounts', @SourceTableJSONStructure='{"AccountID":"91cbd96e-d8d1-428f-8be3-8460a9cbd461","Code":"100","CurrencyCode":"","Name":"Uncleared Funds at Conversion","Status":"ACTIVE","Type":"CURRENT","TaxType":"BASEXCLUDED","Description":"","Class":"ASSET","EnablePaymentsToAccount":false,"ShowInExpenseClaims":false,"SystemAccount":"","BankAccountType":"","BankAccountNumber":"","ReportingCode":"ASS","ReportingCodeName":"","HasAttachments":false,"UpdatedDateUTC":"/Date(1562304669850+0000)/","AddToWatchlist":false}' , @SourceTableQuery='SELECT AccountID, AddToWatchlist, BankAccountNumber, BankAccountType, Class, Code, CurrencyCode, Description, EnablePaymentsToAccount, HasAttachments, Name, ReportingCode, ReportingCodeName, ShowInExpenseClaims, Status, SystemAccount, TaxType, Type, UpdatedDateUTC FROM {0}' , @SourceConfig='direct' ;
GO


