ALTER PROC lepton_dw.sp_ChangeDistribution
( 
 @SCHEMANAME VARCHAR(10),
 @TABLENAME VARCHAR(100),
 @TARGET_DISTRIBUTION VARCHAR(100)
)
--#######################################################################
--
--
--
--#######################################################################
AS 
BEGIN
	--DECLARE @SCHEMANAME VARCHAR(100)
	--DECLARE @TABLENAME VARCHAR(100)
	--DECLARE @TARGET_DISTRIBUTION VARCHAR(100)

	DECLARE @TEMPTABLENAME VARCHAR(100)
	DECLARE @SQL VARCHAR(MAX)

	--SET @SCHEMANAME			 = 'dw'
	--SET @TABLENAME		     = 'deal_sub_type_dim'
	SET @TEMPTABLENAME       =  @TABLENAME + '_tmp'
	--SET @TARGET_DISTRIBUTION = 'REPLICATE'

	DECLARE @DISTRIBUTION_POLICY_DESC VARCHAR(100)
	SELECT @DISTRIBUTION_POLICY_DESC = DISTRIBUTION_POLICY_DESC FROM SYS.PDW_TABLE_DISTRIBUTION_PROPERTIES WHERE OBJECT_ID IN (OBJECT_ID(@SCHEMANAME +'.'+@TABLENAME))
	PRINT(' --SCHEMA                           : ' + @TABLENAME)
	PRINT(' --TABLE                            : '  + @SCHEMANAME)
	PRINT(' --DISTRIBUTION BEFORE MODIFICATION : ' + @DISTRIBUTION_POLICY_DESC)
	PRINT('                                                                  ')

	IF @DISTRIBUTION_POLICY_DESC <> @TARGET_DISTRIBUTION
	BEGIN
		----CREATE TEMP TABLE
		SET @SQL = ' CREATE TABLE ' + @SCHEMANAME + '.' + @TEMPTABLENAME + '
					 WITH
					 (
					   DISTRIBUTION = REPLICATE
					  ,CLUSTERED COLUMNSTORE INDEX
					 )
					 AS
					 SELECT  *  FROM   ' + @SCHEMANAME + '.' + @TABLENAME + ';' 
		PRINT(@SQL)
			
		SET @SQL = 'DROP TABLE ' + @SCHEMANAME + '.' + @TABLENAME + ';'
		PRINT(@SQL)
		    
		SET @SQL = 'RENAME OBJECT ' + @SCHEMANAME + '.' + @TEMPTABLENAME + ' TO ' + @TABLENAME;
		PRINT(@SQL)

	    PRINT('-------------------------------------------------------------------')

		--VALIDATION
		--SELECT @DISTRIBUTION_POLICY_DESC = DISTRIBUTION_POLICY_DESC FROM SYS.PDW_TABLE_DISTRIBUTION_PROPERTIES WHERE OBJECT_ID IN (OBJECT_ID(@SCHEMANAME +'.'+@TABLENAME))
		--PRINT('--DISTRIBUTION BEFORE MODIFICATION : ' + @DISTRIBUTION_POLICY_DESC)
	END
END

GO

IF OBJECT_ID('tempdb..#ChangeDistribution') IS NOT NULL
BEGIN
    DROP TABLE #ChangeDistribution
END

--Create table to hold the tables to modify the distribution
CREATE TABLE #ChangeDistribution
(
 SCHEMANAME VARCHAR(10),
 TABLENAME VARCHAR(100),
 TARGETDISTRIBUTION VARCHAR(100),
 [STATUS] VARCHAR(10),
 [MODIFIEDDATE] DATETIME 
)

--Load all the DW tables into the above Table which needs the distribution change
INSERT INTO #ChangeDistribution(SCHEMANAME, TABLENAME, TARGETDISTRIBUTION, [STATUS], [MODIFIEDDATE]) 
SELECT  'dw', 'gas_location_hierarchy_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'impact_result_action_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'impact_type_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'legal_entity_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'party_agreement_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'portfolio_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'portfolio_hierarchy_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'portfolio_logical_transfer_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_commodity_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_location_hierarchy_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_point_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_pool_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_pool_product_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_product_hour_range_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_profit_center_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'pwr_publication_index_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'sap_message_header_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'scenario_quote_date_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'src_system_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'sub_ledger_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'time_zone_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'trade_index_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'trade_limit_ref_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'trade_user_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'uom_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'user_generic_controls_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'volatility_dim', 'REPLICATE','' , GETDATE() UNION
SELECT  'dw', 'volume_type_dim', 'REPLICATE','' , GETDATE()

--Orchestration
DECLARE @Loop INT;
SET @Loop = 1

SELECT * FROM #ChangeDistribution

WHILE @Loop < (SELECT COUNT(1) FROM #ChangeDistribution)
BEGIN
  DECLARE @SCHEMANAME VARCHAR(10)
  DECLARE @TABLENAME VARCHAR(100)
  DECLARE @TARGETDISTRIBUTION VARCHAR(20)

  SELECT @SCHEMANAME = SCHEMANAME, @TABLENAME = TABLENAME, @TARGETDISTRIBUTION = TARGETDISTRIBUTION  FROM 
			(SELECT row_number() over (order by tablename desc) RNO, SCHEMANAME, TABLENAME, TARGETDISTRIBUTION FROM #ChangeDistribution) a WHERE RNO = @Loop
  
  Exec lepton_dw.sp_ChangeDistribution @SCHEMANAME,@TABLENAME,@TARGETDISTRIBUTION

  SET @Loop = @Loop + 1
END
GO
