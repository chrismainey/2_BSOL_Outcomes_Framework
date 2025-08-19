
/*==================================================================================================================================================================
OUTCOMES FRAMEWORK
(SUS based indicators numerator actuals)

DASR indicators:
	ID Ref      
	10  21001	Emergency Hospital Admissions for Intentional Self-Harm
	11  22401	Emergency Hospital Admissions due to a fall in adults aged over 65yrs
	24  92302	Emergency Hospital Admissions for COPD (35+)
	49  93229	Emergency Hospital Admissions for Coronary Heart Disease (CHD)
	50  93231	Emergency Hospital Admissions for Stroke
	51  93232	Emergency Hospital Admissions for Myocardial Infarction (Heart Attack)
	58  93753   Admissions for Smoking 
	59  93764   Admissions for Alcohol-related Conditions
	109 90808   Hospital Admissions due to Substance Misuse (15 to 24 years)
	114 41401   Admissions for hip fractures (65+ years)
	115 90813   Admissions for Intentional Self-Harm (10-24 years)
	124 93575   Emergency Hospital Admissions for Respiratory Diseases

Crude rate indicators:
	ID Ref 
	19 90810	Hospital admissions caused by asthma in children (under 19 years)
	31 92622	Emergency Hospital Admissions for diabetes (under 19 years)
	32 92623	Admissions for epilepsy (under 19 years)


==================================================================================================================================================================*/


/*==============================================================================================================================================================
DECLARE START AND END MONTHS
==============================================================================================================================================================*/

DECLARE			@start_month	INT
DECLARE			@end_month		INT
SET				@start_month =	201904
SET				@end_month	 =	CAST(FORMAT(GETDATE(), 'yyyyMM') AS INT);

/*==============================================================================================================================================================
CRUDE RATE INDICATORS
==============================================================================================================================================================*/
-- We can use the metadata table to identify crude rate indicators along with their age groupings

DROP TABLE IF EXISTS #crude_rate_indicators

CREATE TABLE #crude_rate_indicators
(
    [indicator_id] INT,
    [age_group] VARCHAR(10)
)

INSERT INTO #crude_rate_indicators (indicator_id, age_group)
VALUES (19, '0-18'), (31, '0-18'), (32, '0-18');


/*==================================================================================================================================================================
CREATE TEMP STAGING DATA TABLES
=================================================================================================================================================================*/

DROP TABLE IF EXISTS   	#BSOL_OF_tbIndicator_PtsCohort_IP
DROP TABLE IF EXISTS   	#BSOL_OF_tbStaging_NumeratorData
DROP TABLE IF EXISTS   	#BSOL_OF_tbStaging_SUS_Data

CREATE TABLE			#BSOL_OF_tbIndicator_PtsCohort_IP (episode_id BIGINT NOT NULL)

CREATE TABLE			#BSOL_OF_tbStaging_NumeratorData 

(						[indicator_id]			INT
,						[reference_id]			VARCHAR (20)
,						[time_period]			INT
,						[financial_year]		VARCHAR (7)
,						[ethnicity_code]		VARCHAR (5)
,						[gender]				VARCHAR (50)
,						[age]					INT
,						[lsoa_2011]				VARCHAR (9)	
,						[lsoa_2021]				VARCHAR (9)
,						[ward_code]				VARCHAR (9)
,						[ward_name]				VARCHAR (55)
,						[locality_res]			VARCHAR (10)
,						[gp_practice]			VARCHAR (10)
,						[numerator]				FLOAT
,						[lad_code]				VARCHAR (9)	
,						[lad_name]				VARCHAR (10)
,						[episode_id]			BIGINT NOT NULL 
)

CREATE TABLE			#BSOL_OF_tbStaging_SUS_Data 

(						
						[indicator_id] INT
,						[start_date] DATE
,						[end_date] DATE
,						[numerator] FLOAT
,						[denominator] FLOAT
,						[indicator_value] FLOAT
,						[lower_ci95] FLOAT
,						[upper_ci95] FLOAT
,						[imd_code] VARCHAR (10)
,						[aggregation_id] VARCHAR(12)
,						[age_group_code] VARCHAR(10)
,						[sex_code] VARCHAR(10)
,						[ethnicity_code] VARCHAR(10)
,						[creation_date] DATE
,						[value_type_code] VARCHAR(10)
,						[source_code] VARCHAR(10)
)



 /*==================================================================================================================================================================
INSERT Inpatient Admission Episode Ids into Staging temp table for all Birmingham and Solihull Residents
=================================================================================================================================================================*/

INSERT			INTO #BSOL_OF_tbIndicator_PtsCohort_IP   (episode_id)
(
SELECT			T1.EpisodeID
FROM			[EAT_Reporting].[dbo].[tbInpatientEpisodes] T1
INNER JOIN		[EAT_Reporting].[dbo].[tbIPPatientGeography] T2		        --New Patient Geography for Inpatients dataset following Unified SUS switch over
ON				T1.EpisodeId = T2.EpisodeId

WHERE			ReconciliationPoint BETWEEN  @start_month AND @end_month
AND				T2.OSLAUA  IN ('E08000025', 'E08000029')					--Bham & Solihull LA

)


/*======================================================================================================================================================================
IndicatorID: 10
ReferenceID: 21001	Emergency Hospital Admissions for Intentional Self-Harm

The number of first finished emergency admission episodes in patients (episode number equals 1, admission method starts with 2), 
with a recording of self harm by cause code (ICD10 X60 to X84) in financial year in which episode ended. 
Regular and day attenders have been excluded. Regions are the sum of the Local Authorities. England is the sum of all Local Authorities 
and admissions coded as U (England NOS).

Numerator Extraction: Emergency Hospital Admissions for Intentional Self Harm. Counts of first finished consultant episodes with an external cause of intentional self harm 
and an emergency admission method were extracted from HES. 
First finished consultant episode counts (excluding regular attenders) were summed in an excel pivot table filtered for emergency admission method 
and separated by quinary age for all ages, sex and local authority in the respective financial year. 

Self harm is defined by external cause codes (ICD10 X60 to X84) which include: 
• Intentional self poisoning (X60 to X69 inclusive), 
• Intentional self harm by hanging, drowning or jumping (X70, X71 and X80), 
• Intentional self harm by firearm or explosive (X72 to X75 inclusive), 
• Intentional self harm using other implement (X78 and X79) 
• Intentional self harm other (X76, X77 and X81 to X84) 
Please note this definition does not include events of undetermined intent.

Numerator Aggregation or allocation: Local Authority of residence of each Finished Admission Episode is allocated by HES. 
			
===========================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			10								AS [indicator_id]
,				'21001'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency admissions
AND				T3.OrderInSpell =1														--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,3) IN ('X60','X61','X62','X63','X64','X66','X67','X68','X69'
						,'X70','X71','X72','X73','X74','X75','X76','X77','X78','X79'
						,'X80','X81','X82','X83','X84')                                  --Self Harm

GROUP BY		T1.episode_id
)

/*==================================================================================================================================================================
IndicatorID: 11
referenceID: 22401 	Emergency hospital admissions due to a fall in adults aged over 65yrs
			
Definition of Numerator: 
Emergency admissions for falls injuries classified by primary diagnosis code (ICD10 code S00 to T98) and external cause (ICD10 code W00 to W19) 
and an emergency admission code (episode order number equals 1, admission method starts with 2). Age at admission 65 and over.
=================================================================================================================================================================*/

DROP TABLE IF EXISTS    #BSOLBI_0033_OF_22401_EM_Falls_65andOver

SELECT			11									    AS [indicator_id]
,               '22401'									AS [reference_id]
,				T1.episode_id

INTO			#BSOLBI_0033_OF_22401_EM_Falls_65andOver

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2				--Emergency Admissions
AND				T3.AgeOnAdmission >= 65							--Age 65 & Over at time of admission
AND				T3.OrderInSpell =1								--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,1) IN ('S','T')			--Falls injuries classified by primary diagnosis code (ICD10 code S00 to T98) 
AND				T2.DiagnosisOrder = 1							--Primary Diagnosis Position

GROUP BY		T1.episode_id

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #combined_output
 
SELECT		T1.*
,			ROW_NUMBER() OVER(PARTITION BY t1.episode_id ORDER BY t1.episode_id) AS Dedup
,			T2.DiagnosisCode AS Secondary_Diagnosis
 
INTO		#combined_output
 
FROM		#BSOLBI_0033_OF_22401_EM_Falls_65andOver T1
INNER JOIN	EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON			T1.episode_id = T2.EpisodeId
WHERE		1=1
AND			LEFT(T2.DiagnosisCode,2) IN ('W0','W1')				--with a Secondary Diagnosis of W00 to W19
 
DELETE FROM #combined_output WHERE Dedup <> 1

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id				
,				reference_id
,				episode_id
,				numerator
)
(
SELECT			indicator_id
,				reference_id
,				episode_id
,				SUM(1) as Numerator
FROM			#combined_output
GROUP BY		indicator_id
,				reference_id
,				episode_id
)

/*==================================================================================================================================================================
IndicatorID: 24
ReferenceID: 92302	Emergency Hospital Admissions for COPD (35+)

Definition of Numerator: 
  - Emergency Admissions with an admission method (ADMIMETH in list '21', '22', '23', '24', '25', '28', '2A','2B', '2C', '2D')
  - Defined by a three digit primary diagnosis (ICD-10: J40-J44)
  - Aged 35+
  - Episode number equals 1
  - Birmingham or Solihull LA Code

=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			 24                             AS [indicator_id]
,				'92302'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency Admissions
AND				T3.OrderInSpell =1														--First Episode in Spell
AND				T2.DiagnosisCode IN (	SELECT	ICD10Code
										FROM	Reference.dbo.DIM_tbICD10
										WHERE	LEFT(ICD10Code,3) LIKE 'J4[01234]'	--COPD
										GROUP BY ICD10Code
									)
AND				T3.AgeOnAdmission >= 35                                                 --Age 35+
AND             T2.DiagnosisOrder = 1													--Primary Diagnosis position

GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID: 49
ReferenceID: 93229	Emergency Hospital Admissions for Coronary Heart Disease (CHD)

Definition of Numerator: 
 -Emergency Admissions with an admission method (ADMIMETH in list '21', '22', '23','24', '25', '28', '2A', '2B', '2C', '2D'); 
 -Defined by a three digit primary diagnosis (ICD10) code of I20, I21, I22, I23, I24 or I25,
 -patient classification 'ordinary' (1 or 2),
  -epiorder is equal to 1
  -----episode status is equal to 3 <<<<<<<<<<<-- Check what this means?
=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			49                              AS [indicator_id]
,				'93229'							AS [reference_id]
,				T1.episode_id
,				SUM (1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id= T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2												--Emergency Admissions
AND				T3.PatientClassificationCode IN (1,2)											--patient classification Ordinary/Daycase Admission
AND				T3.OrderInSpell =1																--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,3) IN  ('I20','I21', 'I22', 'I23', 'I24','I25')			--Coronary Heart Disease (CHD)
AND				T2.DiagnosisOrder = 1															--Primary Diagnosis position

GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID: 50
ReferenceID: 93231	Emergency Hospital Admissions for Stroke

Definition of Numerator: 
 -Emergency Admissions with an admission method (ADMIMETH in list '21', '22', '23', '24', '25', '28', '2A','2B', '2C', '2D'); 
 -Defined by a three digit primary diagnosis (ICD10) codes of I61, I62, I63, I64
 -Patient classification 'ordinary' (1 or 2)

=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			50                              AS [indicator_id]
,				'93231'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency Admissions
AND				T3.PatientClassificationCode IN (1,2)									--patient classification Ordinary/Daycase Admission
AND				T3.OrderInSpell =1														--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,3) IN ('I61','I62','I63','I64')					--Stroke
AND				T2.DiagnosisOrder = 1													--Primary Diagnosis position

GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID  59
ReferenceID  93764 - Admission episodes for alcohol-related conditions (Narrow)

Admissions to hospital where the primary diagnosis is an alcohol-attributable code or a secondary diagnosis is an alcohol-attributable external cause code. 
Directly age standardised rate per 100,000 population (standardised to the European standard population).

Alcohol-related hospital admissions are used as a way of understanding the impact of alcohol on the health of a population.  
There are two measures used in LAPE and elsewhere to assess this burden: the Broad and the Narrow measure.

Narrow definition: A measure of hospital admissions where the primary diagnosis (main reason for admission) is an alcohol-related condition.  
This represents a Narrower measure. Since every hospital admission must have a primary diagnosis it is less sensitive to coding practices but may also understate the part alcohol plays in the admission.

In general, the Broad measure gives an indication of the full impact of alcohol on hospital admissions and the burden placed on the NHS. 
The Narrow measure estimates the number of hospital admissions which are primarily due to alcohol consumption and provides the best indication of trends in alcohol-related hospital admissions.


More specifically, hospital admissions records are identified where the admission is a finished episode; 
it is an admission episode ;
the sex of the patient is valid;
the episode end date falls within the financial year, 
and an alcohol-attributable ICD10 code appears in the primary diagnosis field [diag_01] or an alcohol-related external cause code appears in any diagnosis field [diag_nn].

Wholly Attributable ICD10 codes
https://assets.publishing.service.gov.uk/media/5ee9e2ebd3bf7f1eb35c30f5/APPEND_3-update.pdf

=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			59                              AS [indicator_id]
,				'93764'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				T3.OrderInSpell = 1														    --First Episode in Spell
AND				T2.DiagnosisOrder = 1														--Primary Diagnosis	
AND				T3.AdmissionMethodCode IN (1,2,5)											--AdmissionMethodCode
AND				T2.DiagnosisCode IN ('E244','F100','F101','F102','F103','F104','F105','F106','F107','F108','F109'
									,'G312','G621','B721','I426','K292','K700','K701','K702','K703','K704','K705'
									,'K706','K707','K708','K709','K860','T510','T511','T519','X450','X451','X452'
									,'X453','X454','X455','X456','X457','X458','X459','X650','X651','X652','X653'
									,'X654','X655','X656','X657','X658','X659','Y150','Y151','Y152','Y153','Y154'
									,'Y155','Y156','Y157','Y158','Y159','K852','Q860','R780','Y900','Y901','Y902'
									,'Y903','Y904','Y905','Y906','Y907','Y908','Y909','Y910','Y911','Y912','Y913'
									,'Y914','Y915','Y916','Y917','Y918','Y919')				--Wholly attributable conditions

GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID: 109
ReferenceID:  90808 - Hospital admissions due to substance misuse (15 to 24 years).
Number of admissions where the primary diagnosis is one of the following:

F11 Mental and behavioural disorders due to use of opioids
F12 Mental and behavioural disorders due to use of cannabinoids
F13 Mental and behavioural disorders due to use of sedatives or hypnotics.
F14 Mental and behavioural disorders due to use of cocaine.
F15 Mental and behavioural disorders due to use of other stimulants, including caffeine.
F16 Mental and behavioural disorders due to use of hallucinogens.
F17  Mental and behavioural disorders due to use of tobacco.
F18 Mental and behavioural disorders due to use of volatile solvents.
F19  Mental and behavioural disorders due to multiple drug use and use of other psychoactive substances.
T40 Poisoning by narcotics and psychodysleptics [hallucinogens].
T52 Toxic effect of organic solvents.
T59 Toxic effect of other gases, fumes and vapours.
T43.6 Poisoning by psychotropic drugs, not elsewhere classified - psychostimulants with abuse potential.

Or the main cause (defined as the first diagnosis code that represents an external cause (V01-Y98)) is one of the following:

Y12 Poisoning by and exposure to narcotics and psychodysleptics [hallucinogens], not elsewhere classified, undetermined intent.
Y16 Poisoning by and exposure to organic solvents and halogenated hydrocarbons and their vapours, undetermined intent.
Y19 Poisoning by and exposure to other and unspecified chemicals and noxious substances, undetermined intent.
=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			109                             AS [indicator_id]
,				'90808'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

INNER JOIN      [Reference].[dbo].[DIM_tbICD10] T4
ON			    T2.DiagnosisCode = T4.ICD10Code

WHERE			1=1
AND 			T2.DiagnosisOrder = 1											        -- Primary Diagnosis
AND				T3.OrderInSpell = 1														-- First Episode in Spell
AND				T3.AgeOnAdmission BETWEEN 15 AND 24                                     -- Age between 15 and 24
AND (
				LEFT(T4.ICD10Code, 3) LIKE 'F1[1-9]'
			OR	LEFT(T4.ICD10Code, 3) IN ('T40','T52','T59','Y12','Y16','Y19')
			OR	LEFT(T4.ICD10Code, 4) = 'T436'
    )

GROUP BY		T1.episode_id

)


/*==================================================================================================================================================================
IndicatorID: 114
ReferenceID:  41401 - Reduce hip fractures in people age 65yrs and over		

 The number of first finished emergency admission episodes in patients aged 65 and over at the time 
 of admission (episode order number equals 1, admission method starts with 2), with a recording of 
 fractured neck of femur classified by primary diagnosis code (ICD10 S72.0 Fracture of neck of femur;
 S72.1 Pertrochanteric fracture and S72.2 Subtrochanteric fracture) in financial year in which episode ended.
=================================================================================================================================================================*/


INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			114                             AS [indicator_id]
,				'41401'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND 			LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency Admissions
AND				T3.OrderInSpell =1														--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,4) like ('S72[0-2]')                              --Hip Fractures
AND				T3.AgeOnAdmission >= 65                                                 --Age 65+


GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID: 115
ReferenceID:  90813 - Hospital admissions due to self-harm (10 to 24 years).
Number of admissions where the primary diagnosis is one of the following:

Number of finished admission episodes in children aged between 10 and 24 years where the main recorded cause (defined as the first diagnosis code that represents 
an external cause (V01-Y98)) is between X60 and X84 (Intentional self-harm)
=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			115                             AS [indicator_id]
,				'90813'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId


WHERE			1=1
--AND 			LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency Admissions
AND				T3.OrderInSpell =1														--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,3) IN ('X60','X61','X62','X63','X64','X66','X67','X68','X69'
						,'X70','X71','X72','X73','X74','X75','X76','X77','X78','X79'
						,'X80','X81','X82','X83','X84')                                 --Self Harm
AND				AgeOnAdmission >= 10													--10-24 years
AND				AgeOnAdmission <25														--10-24 years

GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID: 124
ReferenceID: 93575	- emergency admissions for respiratory disease

Definition of Numerator: 
 - Emergency Admissions with an admission method (ADMIMETH in list '21', '22', '23', '24', '25', '28', '2A','2B', '2C', '2D')
  - Defined by a three digit primary diagnosis (ICD10 codes 'J00' to 'J99')
  - Episode number equals 1
  - All Ages

=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			124                             AS [indicator_id]
,				'93575'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency Admissions
AND				T3.OrderInSpell =1														--First Episode in Spell											
AND				T2.DiagnosisCode IN (	SELECT	 ICD10Code
										FROM	 Reference.dbo.DIM_tbICD10
										WHERE	 LEFT(ICD10Code,1) ='J'					--respiratory disease
										GROUP BY ICD10Code
									)

GROUP BY		T1.episode_id

)
/*==================================================================================================================================================================
IndicatorID: 19
ReferenceID: 90810	Hospital admissions caused by asthma in children <19yrs

Definition of Numerator: 
  - Emergency Admissions with an admission method (ADMIMETH in list '21', '22', '23', '24', '25', '28', '2A','2B', '2C', '2D')
  - Defined by a three digit primary diagnosis of either J45:Asthma or J46:Status asthmaticus
  - Aged < 19 yo
  - Episode number equals 1
  - Birmingham or Solihull LA Code

=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			 19								AS [indicator_id]
,				'90810'							AS [reference_id]
,				T1.episode_id
,				SUM(1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id= T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2										--Emergency Admissions
AND				T3.OrderInSpell =1														--First Episode in Spell
AND				T2.DiagnosisCode IN (	SELECT	ICD10Code
										FROM	Reference.dbo.DIM_tbICD10
										WHERE	LEFT(ICD10Code,3) LIKE 'J4[56]'			--Asthma or Status asthmaticus
										GROUP BY ICD10Code
									)

AND				T3.AgeOnAdmission < 19                                                 --Age <19
AND             T2.DiagnosisOrder = 1													--Primary Diagnosis position


GROUP BY		T1.episode_id

)
/*==================================================================================================================================================================
IndicatorID 31
ReferenceID 92622	Admissions for Diabetes (under 19 years)
		
Definition of Numerator: 
Emergency hospital admissions of children and young people aged under 19 years with primary diagnosis of E10: Insulin-dependent diabetes mellitus. 
The number of finished emergency admissions (episode number equals 1, admission method starts with 2)
=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			31								AS [indicator_id]
,				'92622'							AS [reference_id]
,				T1.episode_id
,				SUM (1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2				--Emergency Admissions
AND				T3.AgeOnAdmission < 19							--Children and young people aged under 19 years
AND				T3.OrderInSpell =1								--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,3) IN ('E10')				--Insulin-dependent diabetes mellitus
AND				T2.DiagnosisOrder = 1							--Primary Diagnosis position

GROUP BY		T1.episode_id

)

/*==================================================================================================================================================================
IndicatorID: 32
ReferenceID: 92623	- Admissions for epilepsy (under 19 years)
		
Definition of Numerator: 
Emergency hospital admissions of children and young people aged under 19 years with primary diagnosis of G40 (epilepsy) or G41 (Status epilepticus). 
The number of finished emergency admissions (episode number equals 1, admission method starts with 2)
=================================================================================================================================================================*/

INSERT INTO		#BSOL_OF_tbStaging_NumeratorData
(				indicator_id
,				reference_id
,				episode_id
,				numerator
)

(
SELECT			31								AS [indicator_id]
,				'92622'							AS [reference_id]
,				T1.episode_id
,				SUM (1)							AS [numerator]	

FROM			#BSOL_OF_tbIndicator_PtsCohort_IP T1

INNER JOIN		EAT_Reporting.dbo.tbIpDiagnosisRelational T2
ON				T1.episode_id = T2.EpisodeID	

INNER JOIN		EAT_Reporting.dbo.tbInpatientEpisodes T3
ON				T1.episode_id = T3.EpisodeId

WHERE			1=1
AND				LEFT(T3.AdmissionMethodCode,1) = 2				--Emergency Admissions
AND				T3.AgeOnAdmission < 19							--Children and young people aged under 19 years
AND				T3.OrderInSpell =1								--First Episode in Spell
AND				LEFT(T2.DiagnosisCode,3) IN ('G40', 'G41')		--Epilepsy or Status epilepticus
AND				T2.DiagnosisOrder = 1							--Primary Diagnosis position

GROUP BY		T1.episode_id

)
/*==================================================================================================================================================================
UPDATE TimePeriod, Age and GP Practice from Source data at time of admission
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[time_period]	= T2.[ReconciliationPoint]
,			T1.[age]			= T2.[AgeonAdmission]
,			T1.[gp_practice]	= T2.[GMPOrganisationCode]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting].[dbo].[tbInpatientEpisodes] T2
ON			T1.[episode_id] = T2.[EpisodeId] --67,094

/*==================================================================================================================================================================
UPDATE Gender from Source data at time of admission
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[gender]			= T3.[GenderDescription]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting].[dbo].[tbInpatientEpisodes] T2
ON			T1.[episode_id] = T2.[EpisodeId]

LEFT JOIN	[Reference].[dbo].[DIM_tbGender] T3
ON			T2.GenderCode = T3.GenderCode


/*==================================================================================================================================================================
UPDATE LSOA_2011 from CSU Patient Geography data table
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[lsoa_2011]	= T2.[LowerlayerSuperOutputArea2011]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting].[dbo].[tbIPPatientGeography] T2		--New Patient Geography for Inpatients dataset following Unified SUS switch over

ON			T1.[episode_id] = T2.[EpisodeId]


/*==================================================================================================================================================================
UPDATE LSOA_2021 
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[lsoa_2021]	= T2.[LowerlayerSuperOutputArea2021]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting].[dbo].[tbIPPatientGeography] T2		--New Patient Geography for Inpatients dataset following Unified SUS switch over

ON			T1.[episode_id] = T2.[EpisodeId]


/*==================================================================================================================================================================
UPDATE WardCode from local LSOA21 to Ward22 mapping table
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[ward_code]	= T2.[WD22CD]
,			T1.[ward_name]	= T2.[WD22NM]
,			T1.[lad_code]	= T2.[LAD22CD]
,			T1.[lad_name]   = T2.[LAD22NM]
 
FROM		#BSOL_OF_tbStaging_NumeratorData T1
 
INNER JOIN	[EAT_Reporting_BSOL].[Reference].[LSOA_2021_WARD_LAD] T2
ON			T1.[lsoa_2021] = T2.[LSOA21CD]



/*==================================================================================================================================================================
UPDATE Locality from local LSOA21 to Locality mapping table
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[locality_res]	= T2.[Locality]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting_BSOL].[Reference].[LSOA_2021_BSOL_to_Constituency_2025_Locality] T2
ON			T1.[lsoa_2021] = T2.[LSOA21CD]


-- TO DO: Use locality codes from the geography reference table, will need to join with the reference table instead of manually updating 
UPDATE T1

SET T1.[locality_res] = CASE 
	WHEN T1.[locality_res]   = 'Solihull' THEN 'BSOL001'
	WHEN T1.[locality_res]   = 'Central'  THEN 'BSOL002'
	WHEN T1.[locality_res]   = 'North'    THEN 'BSOL003'
	WHEN T1.[locality_res]   = 'East'     THEN 'BSOL004'
	WHEN T1.[locality_res]   = 'South'    THEN 'BSOL005'
	WHEN T1.[locality_res]   = 'West'     THEN 'BSOL006'
	ELSE T1.[locality_res] 
END

FROM #BSOL_OF_tbStaging_NumeratorData T1

/*==================================================================================================================================================================
UPDATE Ethnicity from local Ethnicity Demographic table and then from SUS where NULL
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[ethnicity_code]	= T3.[Ethnic_Code]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting].[dbo].[tbInpatientEpisodes] T2
ON			T1.[episode_id] = T2.[EpisodeId]

INNER JOIN	EAT_Reporting_BSOL.Demographic.Ethnicity T3
ON			T2.[NHSNumber] = T3.[Pseudo_NHS_Number]

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

--UPDATES ANY MISSING ETHNICITY FROM RAW SOURCE DATA (i.e. SUS)

UPDATE		T1
SET			T1.[ethnicity_code]	= T2.[EthnicCategoryCode] 

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[EAT_Reporting].[dbo].[tbInpatientEpisodes] T2
ON			T1.[episode_id] = T2.[EpisodeId]

WHERE		T1.Ethnicity_Code IS NULL

-- CLEAN ETHNICITY CODES
UPDATE		 T1
SET			 T1.[ethnicity_code] = 
    CASE 
        WHEN T1.[ethnicity_code] = '9' THEN '99' --Unknown
        WHEN T1.[ethnicity_code] = 'A*' THEN 'A' -- British
        WHEN T1.[ethnicity_code] = 'ZZ' THEN 'Z' -- Not stated
		WHEN T1.[ethnicity_code] IS NULL THEN '99' -- Unknown
        ELSE T1.[ethnicity_code] -- Keep the existing value if no condition matches
    END
from		#BSOL_OF_tbStaging_NumeratorData t1

/*==================================================================================================================================================================
UPDATE FinancialYear 
=================================================================================================================================================================*/

UPDATE		T1
SET			T1.[financial_year]	= T2.[HCSFinancialYearName]

FROM		#BSOL_OF_tbStaging_NumeratorData T1

INNER JOIN	[Reference].[dbo].[DIM_tbDate] T2
ON			T1.[time_period] = T2.[HCCSReconciliationPoint]


/*==================================================================================================================================================================
UPDATE columns to match the codes from the reference tables
=================================================================================================================================================================*/
DROP TABLE IF EXISTS #combined_datasets

SELECT			T1.[indicator_id]
,				T1.[financial_year]
,				T1.[numerator]
,				CAST(T2.[quintile] AS VARCHAR(10)) AS imd_code
,				T1.[ethnicity_code]
,				T1.[gender] AS sex_code
,				CAST([age] AS varchar(10)) AS age_group_code
,				T1.[lsoa_2011]
,				T1.[lsoa_2021]
,				T1.[ward_code]
,				[locality_res] AS locality_res_code
,				[lad_code]
,				[episode_id]
INTO			#combined_datasets
FROM			#BSOL_OF_tbStaging_NumeratorData T1
LEFT JOIN		[EAT_Reporting_BSOL].[OF].[OF2_Reference_Ward_To_IMD] T2 -- Using Ward to IMD quintile lookup to update the IMD quintile column, which is based on the population-weighted average IMD scores across LSOAs within each Ward
ON				T1.[ward_code] = T2.[ward_code]


--IMD

UPDATE		T1
SET			T1.[imd_code] = T2.[imd_code]

FROM		#combined_datasets T1

INNER JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_IMD] T2
ON			T1.[imd_code] = T2.[imd_quintile]

-- ETHNICITY

UPDATE		T1
SET			T1.[ethnicity_code] = T2.[ethnicity_code]

FROM		#combined_datasets T1

INNER JOIN [EAT_Reporting_BSOL].[OF].[OF2_Reference_Ethnicity] T2
ON			T1.[ethnicity_code] = T2.[nhs_code]


-- SEX

UPDATE		T1
SET			T1.[sex_code] = '999' -- all (persons)

FROM		#combined_datasets T1

-- AGE GROUP
-- UPDATE age group for the DASR indicators

UPDATE		T1
SET			T1.[age_group_code] = T2.[AgeBand_5YRS]

FROM		#combined_datasets T1

INNER JOIN	[EAT_Reporting_BSOL].[Reference].[tbAge] T2 
ON			T1.[age_group_code] = T2.[Age]

WHERE       T1.[indicator_id] NOT IN (SELECT indicator_id FROM #crude_rate_indicators)

-- UPDATE the max 5-years age bands: 85+ yrs

UPDATE		T1
SET			T1.[age_group_code] = '85+'

FROM		#combined_datasets T1

WHERE       T1.[age_group_code] IN ('85-89', '90-94', '95-99', '100-104', '105-109', '110-114', '115-119', '120-124', '125-129')

-- UPDATE age group for the crude rate indicators

UPDATE		T1
SET			T1.[age_group_code] = T2.[age_group]

FROM		#combined_datasets T1

INNER JOIN	#crude_rate_indicators T2 
ON			T1.[indicator_id] = T2.[indicator_id]

WHERE       T1.[indicator_id]  IN (SELECT indicator_id FROM #crude_rate_indicators)

-- UPDATE unknown age group

UPDATE		T1
SET			T1.[age_group_code] = 'Unknown'

FROM		#combined_datasets T1

WHERE		T1.[age_group_code] IS NULL

-- CONVERT to codes

UPDATE		T1
SET			T1.[age_group_code] = T2.[age_code]

FROM		#combined_datasets T1

INNER JOIN	[EAT_Reporting_BSOL].[OF].[OF2_Reference_Age_Group]  T2 
ON			T1.[age_group_code] = T2.[age_group]

/*==================================================================================================================================================================
INSERT data into SUS staging table 
=================================================================================================================================================================*/

INSERT INTO #BSOL_OF_tbStaging_SUS_Data (
			[indicator_id]
,			[start_date]
,			[end_date]
,			[numerator]
,			[denominator]
,			[indicator_value]
,			[lower_ci95]
,			[upper_ci95]
,			[imd_code]
,			[aggregation_id]
,			[age_group_code]
,			[sex_code]
,			[ethnicity_code]
,			[creation_date]
,			[value_type_code]
,			[source_code]
)

-- Ward Geography
(SELECT		DISTINCT 
			T1.[indicator_id]
,			CAST(LEFT(T1.[financial_year], 4) + '-04-01' AS DATE) AS start_date
,			CAST('20' + RIGHT(T1.[financial_year], 2) + '-03-31' AS DATE) AS end_date
,			SUM(T1.[numerator]) AS numerator
,			CAST(NULL AS INT) AS denominator
,			CAST(NULL AS NUMERIC) AS indicator_value
,			CAST(NULL AS NUMERIC) AS lower_ci95
,			CAST(NULL AS NUMERIC) AS upper_ci95
,			T1.[imd_code]
,			T1.[ward_code] AS aggregation_id
,			T1.[age_group_code]
,			T1.[sex_code] 
,			T1.[ethnicity_code] 
,			CAST(CURRENT_TIMESTAMP AS DATE) AS creation_date
,			CASE 
					WHEN T1.[indicator_id] IN (SELECT indicator_id FROM #crude_rate_indicators) THEN '3'
						ELSE '4' 
					END AS value_type_code 
,			'1' AS source_code -- SQL

FROM		#combined_datasets T1

GROUP BY 
			T1.[indicator_id]
,			CAST(LEFT(T1.[financial_year], 4) + '-04-01' AS DATE)
,			CAST('20' + RIGHT(T1.[financial_year], 2) + '-03-31' AS DATE)
,			T1.[imd_code]
,			T1.[ward_code] 
,			T1.[sex_code]
,			T1.[age_group_code]
,			T1.[ethnicity_code] 

UNION

-- LAD Geography
SELECT		DISTINCT
			T1.[indicator_id]
,			CAST(LEFT(T1.financial_year, 4) + '-04-01' AS DATE) AS start_date
,			CAST('20' + RIGHT(T1.financial_year, 2) + '-03-31' AS DATE) AS end_date
,			SUM(T1.numerator) AS numerator
,			CAST(NULL AS INT) AS denominator
,			CAST(NULL AS NUMERIC) AS indicator_value
,			CAST(NULL AS NUMERIC) AS lower_ci95
,			CAST(NULL AS NUMERIC) AS upper_ci95
,			T1.[imd_code] 
,			T1.[lad_code] AS aggregation_id
,			T1.[age_group_code] 
,			t1.[sex_code] 
,			T1.[ethnicity_code] AS ethnicity_code
,			CAST(CURRENT_TIMESTAMP AS date) AS creation_date
,			CASE 
					WHEN T1.[indicator_id] IN (SELECT indicator_id FROM #crude_rate_indicators) THEN '3'
						ELSE '4' 
					END AS value_type_code
,			'1' AS source_code
FROM		#combined_datasets T1

GROUP BY 
			T1.[indicator_id]
,			CAST(LEFT(T1.[financial_year], 4) + '-04-01' AS DATE)
,			CAST('20' + RIGHT(T1.[financial_year], 2) + '-03-31' AS DATE)
,			T1.[imd_code]
,			T1.[lad_code]
,			T1.[age_group_code]
,			T1.[sex_code]
,			T1.[ethnicity_code] 

UNION 

--Locality Geography
SELECT		DISTINCT
			T1.[indicator_id]
,			CAST(LEFT(T1.[financial_year], 4) + '-04-01' AS DATE) AS start_date
,			CAST('20' + RIGHT(T1.[financial_year], 2) + '-03-31' AS DATE) AS end_date
,			SUM(T1.[numerator]) AS numerator
,			CAST(NULL AS INT) AS denominator
,			CAST(NULL AS NUMERIC) AS indicator_value
,			CAST(NULL AS NUMERIC) AS lower_ci95
,			CAST(NULL AS NUMERIC) AS upper_ci95
,			T1.[imd_code] 
,			T1.[locality_res_code] AS aggregation_id
,			T1.[age_group_code] 
,			T1.[sex_code] 
,			T1.[ethnicity_code]
,			CAST(CURRENT_TIMESTAMP AS DATE) AS creation_date
,			CASE 
					WHEN T1.[indicator_id] IN (SELECT indicator_id FROM #crude_rate_indicators) THEN '3'
						ELSE '4' 
					END AS value_type_code
,			'1' AS source_code -- SQL
FROM		#combined_datasets T1
	
WHERE		[locality_res_code] <> 'Non-bsol'
    
GROUP BY 
			T1.[indicator_id]
,			CAST(LEFT(T1.[financial_year], 4) + '-04-01' AS DATE)
,			CAST('20' + RIGHT(T1.[financial_year], 2) + '-03-31' AS DATE)
,			T1.[imd_code]
,			T1.[locality_res_code]
,			T1.[age_group_code]
,			T1.[sex_code]
,			T1.[ethnicity_code] 


)

/*==================================================================================================================================================================
CREATE dataset for rates by Ethnicity with IMD collapsed to 'All'
=================================================================================================================================================================*/
DROP TABLE IF EXISTS #temp1

SELECT  
			[indicator_id]
,			[start_date]
,			[end_date]
,			SUM([numerator]) AS numerator
,			NULL AS denominator
,			NULL AS indicator_value     
,			NULL AS lower_ci95
,			NULL AS upper_ci95
,			'999' AS imd_code -- All
,			[aggregation_id]
,			[age_group_code]
,			[sex_code]
,			[ethnicity_code]
,			[creation_date]
,			[value_type_code]
,			[source_code]

INTO		#temp1

FROM		#BSOL_OF_tbStaging_SUS_Data

GROUP BY 
			[indicator_id]
,			[start_date]
,			[end_date]
,			[aggregation_id]
,			[age_group_code]
,			[sex_code]
,			[ethnicity_code]
,			[creation_date]
,			[value_type_code]
,			[source_code]
	 
/*==================================================================================================================================================================
Create dataset for rates by IMD with Ethnicity collapsed to 'All'
=================================================================================================================================================================*/
DROP TABLE IF EXISTS #temp2

SELECT  
			[indicator_id]
,			[start_date]
,			[end_date]
,			SUM([numerator]) AS Numerator
,			NULL AS Denominator
,			NULL AS IndicatorValue     
,			NULL AS LowerCI95
,			NULL AS UpperCI95
,			[imd_code]
,			[aggregation_id]
,			[age_group_code]
,			[sex_code]
,			'999' AS [ethnicity_code] -- All
,			[creation_date]
,			[value_type_code]
,			[source_code]

INTO		#temp2

FROM		#BSOL_OF_tbStaging_SUS_Data

GROUP BY 
			[indicator_id]
,			[start_date]
,			[end_date]
,			[imd_code]
,			[aggregation_id]
,			[age_group_code]
,			[sex_code]
,			[creation_date]
,			[value_type_code]
,			[source_code]

/*==================================================================================================================================================================
INSERT final dataset into Indicator SQL Data table
=================================================================================================================================================================*/
DROP TABLE IF EXISTS #final

SELECT * INTO #final
FROM (
    SELECT * FROM #BSOL_OF_tbStaging_SUS_Data
    UNION
    SELECT * FROM #temp1
    UNION
    SELECT * FROM #temp2
) AS Combined;

INSERT INTO [EAT_Reporting_BSOL].[OF].[OF2_Indicator_SQL_Data]
SELECT * FROM #final 


