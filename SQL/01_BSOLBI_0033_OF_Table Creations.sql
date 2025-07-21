  	
/*=================================================================================================
  Table Creations			
=================================================================================================*/	

-- Raw data tables

  -- Table 1 (Data warehouse)
  
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_IndicatorSQLData] (
	
         [IndicatorID] INT
	   , [StartDate] DATE
	   , [EndDate] DATE
	   , [Numerator] FLOAT
	   , [Denominator] FLOAT
	   , [IndicatorValue] FLOAT
	   , [LowerCI95] FLOAT
	   , [UpperCI95] FLOAT
	   , [IMD] VARCHAR(10)
	   , [Geography] VARCHAR(100)
	   , [AgeGroup] VARCHAR(100)
	   , [Sex] VARCHAR(10)
	   , [Ethnicity] VARCHAR(100)
	   , [CreationDate] DATE
	   , [ValueType] VARCHAR(100)
	   , [Source] VARCHAR(10)
	   )


  -- Table 2 (API)
  
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_IndicatorAPIData] (
	
         [IndicatorID] INT
	   , [StartDate] DATE
	   , [EndDate] DATE
	   , [Numerator] FLOAT
	   , [Denominator] FLOAT
	   , [IndicatorValue] FLOAT
	   , [LowerCI95] FLOAT
	   , [UpperCI95] FLOAT
	   , [IMD] VARCHAR(10)
	   , [Geography] VARCHAR(100)
	   , [AgeGroup] VARCHAR(100)
	   , [Sex] VARCHAR(10)
	   , [Ethnicity] VARCHAR(100)
	   , [CreationDate] DATE
	   , [ValueType] VARCHAR(100)
	   , [Source] VARCHAR(10)
	   )

  -- Table 3 (SharePoint)
  
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_IndicatorSharePointData] (
	
         [IndicatorID] INT
	   , [StartDate] DATE
	   , [EndDate] DATE
	   , [Numerator] FLOAT
	   , [Denominator] FLOAT
	   , [IndicatorValue] FLOAT
	   , [LowerCI95] FLOAT
	   , [UpperCI95] FLOAT
	   , [IMD] VARCHAR(10)
	   , [Geography] VARCHAR(100)
	   , [AgeGroup] VARCHAR(100)
	   , [Sex] VARCHAR(10)
	   , [Ethnicity] VARCHAR(100)
	   , [CreationDate] DATE
	   , [ValueType] VARCHAR(100)
	   , [Source] VARCHAR(10)
	   )

  -- Table 4 (Other)
  
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_IndicatorOtherData] (
	
         [IndicatorID] INT
	   , [StartDate] DATE
	   , [EndDate] DATE
	   , [Numerator] FLOAT
	   , [Denominator] FLOAT
	   , [IndicatorValue] FLOAT
	   , [LowerCI95] FLOAT
	   , [UpperCI95] FLOAT
	   , [IMD] VARCHAR(10)
	   , [Geography] VARCHAR(100)
	   , [AgeGroup] VARCHAR(100)
	   , [Sex] VARCHAR(10)
	   , [Ethnicity] VARCHAR(100)
	   , [CreationDate] DATE
	   , [ValueType] VARCHAR(100)
	   , [Source] VARCHAR(10)
	   )

-- Staging table

  -- Table 5
  
    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_IndicatorStagingData] (
	
         [IndicatorID] INT
	   , [StartDate] DATE
	   , [EndDate] DATE
	   , [Numerator] FLOAT
	   , [Denominator] FLOAT
	   , [IndicatorValue] FLOAT
	   , [LowerCI95] FLOAT
	   , [UpperCI95] FLOAT
	   , [IMDCode] VARCHAR (10)
	   , [GeographyCode] VARCHAR(12)
	   , [AgeGroupCode] VARCHAR(10)
	   , [SexCode] VARCHAR(10)
	   , [EthnicityCode] VARCHAR(10)
	   , [CreationDate] DATE
	   , [ValueTypeCode] VARCHAR(10)
	   , [SourceCode] VARCHAR(10)
	   )
	

-- Reference tables

	-- Sex

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Sex] (
         [SexCode] VARCHAR(10)
	   , [Sex] VARCHAR(10)
	   )

    -- Age Group

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_AgeGroup] (
         [AgeGroupCode] VARCHAR(10)
	   , [AgeGroup] VARCHAR(100)
	   )

	-- IMD

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_IMD] (
         [IMDCode] VARCHAR(10)
	   , [IMD] VARCHAR(10)
	   )

	-- Value Type

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_ValueType] (
         [ValueTypeCode] VARCHAR(10)
	   , [ValueType] VARCHAR(100)
	   )

	-- Working Project

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_WorkingProject] (
         [WorkingProjectCode] VARCHAR(10)
	   , [WorkingProject] VARCHAR(10)
	   )

	-- Source

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Source] (
         [SourceCode] VARCHAR(10)
	   , [Source] VARCHAR(12)
	   )


	-- Polarity

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Polarity] (
         [PolarityCode] VARCHAR(10)
	   , [Polarity] VARCHAR(100)
	   )

	-- Geography

    CREATE TABLE  [EAT_Reporting_BSOL].[OF].[OF2_Reference_Geography] (
         [GeographyCode] VARCHAR(10)
	   , [GeographyType] VARCHAR(50)
	   , [GeographyLabel] VARCHAR(100)
	   )


		
	  


 