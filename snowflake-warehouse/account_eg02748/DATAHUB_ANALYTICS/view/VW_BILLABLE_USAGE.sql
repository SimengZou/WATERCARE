create or replace view DATAHUB_ANALYTICS.VW_BILLABLE_USAGE(
	ACCOUNTKEY,
	BILLKEY,
	COMPKEY,
	UNITID,
	SERVSTAT,
	BLUSAGE,
	ESTMFLAG
) as
SELECT DISTINCT a.accountkey,
                b.billkey,
                w.compkey,
                w.unitid,
                w.servstat,
                mr.blusage,
                mr.estmflag
FROM   DATAHUB_TARGET.IPS_billing_bill b
       INNER JOIN DATAHUB_TARGET.IPS_billing_account a
               ON a.accountkey = b.accountkey
       INNER JOIN DATAHUB_TARGET.IPS_billing_accountservice s 
               ON a.accountkey = s.accountkey
       INNER JOIN DATAHUB_TARGET.IPS_billing_serviceoptions so 
               ON s.serviceoptionskey = so.serviceoptionskey
       INNER JOIN DATAHUB_TARGET.IPS_billing_watertrackedreading wtr
               ON wtr.billkey = b.billkey
       INNER JOIN DATAHUB_TARGET.IPS_assetmanagement_water_meterreading mr 
               ON mr.readingkey = wtr.readingkey
       INNER JOIN DATAHUB_TARGET.IPS_assetmanagement_water_compwmtr w 
               ON w.compkey = mr.compkey;