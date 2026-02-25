create or replace view DATAHUB_EXTRACT.VW_SM_CURATED_MEASUREMENT_NEW_IPS_ACCOUNT(
	IPS_ACCOUNTNUMBER,
	M_METER_TIME,
	M_VALUE,
	IPS_CONSUMPTIONPERCENTAGE,
	M_UNIT_ID
) as
with IPS as (SELECT I.IPS_ACCOUNTNUMBER,
                    I.IPS_UNITID,
                    I.IPS_CONSUMPTIONPERCENTAGE
             FROM DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID I 
				INNER JOIN
						(select distinct w.unitid AS UNIT_ID
						from DATAHUB_TARGET.IPS_billing_account a
							inner join DATAHUB_TARGET.IPS_billing_accountservice s  on a.accountkey=s.accountkey
							inner join DATAHUB_TARGET.IPS_billing_accountserviceposition p  on s.accountservicekey=p.accountservicekey
							inner join DATAHUB_TARGET.IPS_assetmanagement_assloc l on p.addrkey=l.addrkey and p.position=l.position and l.remdt is null
							inner join DATAHUB_TARGET.IPS_assetmanagement_water_compwmtr w on l.compkey=w.compkey
						WHERE a.accountstatus = 'A' and
							w.servstat = 'OP') W
							on IPS_UNITID = w.UNIT_ID),
UNITID_NEW_ACCOUNT as (
SELECT DISTINCT
    M_UNIT_ID
    ,M_METER_TIME
    ,M_VALUE
FROM TARGET_SM.SM_CURATED_MEASUREMENT M
WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min' and
    EXISTS 
        (SELECT 1
         FROM DATAHUB_EXTRACT.SM_CURATED_MEASUREMENT_MISSING_IPS_ACCOUNT MIA
                INNER JOIN IPS I
                    on MIA.M_UNIT_ID = I.IPS_UNITID
            WHERE   MIA.HAS_IPS_ACCOUNT = false and M.M_UNIT_ID = MIA.M_UNIT_ID))
SELECT 	IPS.IPS_ACCOUNTNUMBER
        ,concat(to_varchar(dateadd('m',-30,s.M_METER_TIME), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') ,'#',s.M_UNIT_ID) AS M_METER_TIME
        ,M_VALUE*IPS_CONSUMPTIONPERCENTAGE AS M_VALUE
        ,IPS_CONSUMPTIONPERCENTAGE
        ,M_UNIT_ID
FROM UNITID_NEW_ACCOUNT S
    INNER JOIN IPS on IPS.IPS_UNITID = S.M_UNIT_ID