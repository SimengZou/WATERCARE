create or replace view DATAHUB_EXTRACT.VW_SM_STREAM_CURATED_MEASUREMENT(
	IPS_ACCOUNTNUMBER,
	M_METER_TIME,
	M_VALUE,
	IPS_CONSUMPTIONPERCENTAGE,
	M_UNIT_ID,
	HAS_IPS_ACCOUNT
) as
with smq as(
    SELECT  I.IPS_ACCOUNTNUMBER
            ,concat(to_varchar(dateadd('m',-30,s.M_METER_TIME), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') ,'#',s.M_UNIT_ID) as M_METER_TIME
            ,M_VALUE*IPS_CONSUMPTIONPERCENTAGE AS M_VALUE
            ,IPS_CONSUMPTIONPERCENTAGE
            ,M_UNIT_ID
   
            ,CASE WHEN IPS_ACCOUNTNUMBER is not null THEN 1 ELSE 0 END HAS_IPS_ACCOUNT
    FROM
        (SELECT DISTINCT
                M_UNIT_ID
                ,M_METER_TIME
                ,M_VALUE
        FROM DATAHUB_EXTRACT.STREAM_SM_CURATED_MEASUREMENT
        WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'  and  M_PLATFORM_NAME <> 'cumulocity'
        ) S
         JOIN (select
                    distinct
                    a.accountnumber IPS_ACCOUNTNUMBER,
                    c.unitid IPS_UNITID
               ,1 as IPS_CONSUMPTIONPERCENTAGE

                FROM
                    DATAHUB_TARGET.IPS_WSLASSETWATER_XMETERACCOUNT x
                    inner join DATAHUB_TARGET.IPS_billing_account a  on x.account=a.accountkey
                inner join DATAHUB_TARGET.IPS_Assetmanagement_Water_compwmtr c on x.COMPKEY = c.COMPKEY
                where a.accountstatus = 'A' and c.servstat = 'OP'
                and      c.UNITTYPE = 'MTSRD07'
                and      x.SERVICETYPE = 'Water'
                and   x.SHAREPERC = 100) I ON I.IPS_UNITID=S.M_UNIT_ID   
        INNER JOIN
                (select distinct w.unitid AS UNIT_ID
                from DATAHUB_TARGET.IPS_billing_account a
                    inner join DATAHUB_TARGET.IPS_billing_accountservice s  on a.accountkey=s.accountkey
                    inner join DATAHUB_TARGET.IPS_billing_accountserviceposition p  on s.accountservicekey=p.accountservicekey
                    inner join DATAHUB_TARGET.IPS_assetmanagement_assloc l on p.addrkey=l.addrkey and p.position=l.position and l.remdt is null
                    inner join DATAHUB_TARGET.IPS_assetmanagement_water_compwmtr w on l.compkey=w.compkey
                WHERE a.accountstatus = 'A' and
                    w.servstat = 'OP') PILOT
            ON PILOT.UNIT_ID=S.M_UNIT_ID  
)
SELECT DISTINCT s.IPS_ACCOUNTNUMBER,s.M_METER_TIME,s.M_VALUE,s.IPS_CONSUMPTIONPERCENTAGE, M_UNIT_ID, HAS_IPS_ACCOUNT
FROM smq s