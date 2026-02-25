create or replace view VW_IPS_SM_ACCOUNT_UNITID(
	IPS_ACCOUNTNUMBER,
	IPS_CONSUMPTIONPERCENTAGE,
	IPS_UNITID,
	IPS_ACCOUNTS_PER_UNITID_COUNT
) as 
 select distinct
a.ACCOUNTNUMBER AS IPS_ACCOUNTNUMBER
,CASE WHEN p.CONSUMPTIONPERCENTAGE IS  NOT NULL AND p.CONSUMPTIONPERCENTAGE>0 THEN p.CONSUMPTIONPERCENTAGE/100 ELSE 1 END AS IPS_CONSUMPTIONPERCENTAGE
,w.unitid AS IPS_UNITID
,count(ACCOUNTNUMBER) OVER (PARTITION BY w.unitid) AS IPS_accounts_per_unitid_count
from DATAHUB_TARGET.IPS_billing_account a
inner join DATAHUB_TARGET.IPS_billing_accountservice s  on a.accountkey=s.accountkey
inner join DATAHUB_TARGET.IPS_billing_accountserviceposition p  on s.accountservicekey=p.accountservicekey
inner join DATAHUB_TARGET.IPS_assetmanagement_assloc l on p.addrkey=l.addrkey and p.position=l.position and l.remdt is null
inner join DATAHUB_TARGET.IPS_assetmanagement_water_compwmtr w on l.compkey=w.compkey
INNER JOIN DATAHUB_TARGET.IPS_BILLING_SERVICEOPTIONS so ON so.SERVICEOPTIONSKEY=s.SERVICEOPTIONSKEY AND trim(so.code)='Water'
--INNER JOIN (SELECT DISTINCT SMARTMETERID AS unitid FROM DATAHUB_LANDING.SM_APSY_SMINF_PILOT WHERE READYFORLIVE ='y') pilot ON w.unitid =pilot.unitid
WHERE
1=1
AND p.SubtractiveFlag='N';