create or replace view DATAHUB_ANALYTICS.VW_BILL_LINE_AMOUNT(
	ACCOUNTKEY,
	ACCOUNTNUMBER,
	CUSTDESC,
	BILLKEY,
	INVOICEDT,
	TOTALAMOUNTDUE,
	DUEDATE,
	AMTDUE,
	PREVBILLAMT,
	TOTALS,
	CONSUMPTION,
	LINEITEMUSAGE,
	BILLPERFRDATE,
	BILLPERTODATE,
	DESCRIPTION,
	ADDRKEY,
	LINEITEMKEY
) as
SELECT DISTINCT a.accountkey,
                a.accountnumber,
                a.custdesc,
                b.billkey,
                b.billdate                                      InvoiceDt,
                (coalesce(b.billamt, 0) + coalesce(b.arngamt, 0) ) TotalAmountDue,
                duedate                                         DueDate,
                coalesce(b.principaltotalamount, 0)               AmtDue,
                b.priorbillamt                                  PrevBillAmt,
                lineitemamt                                     Totals,
                lineitemunits                                   consumption,
                lineitemusage,
                billperfrdate,
                billpertodate,
                ls.description,
                A.addrkey,
                lineitemkey
FROM   DATAHUB_TARGET.IPS_billing_lineitem l
       INNER JOIN DATAHUB_TARGET.IPS_billing_bill b
               ON b.billkey = l.billkey
       INNER JOIN DATAHUB_TARGET.IPS_billing_account a
               ON a.accountkey = b.accountkey
       INNER JOIN DATAHUB_TARGET.IPS_billing_accountservice s 
               ON l.accountservicekey = s.accountservicekey
       INNER JOIN DATAHUB_TARGET.IPS_billing_serviceoptions so 
               ON s.serviceoptionskey = so.serviceoptionskey
       INNER JOIN DATAHUB_TARGET.IPS_billing_lineitemsetup ls
               ON l.lineitemsetupkey = ls.lineitemsetupkey
WHERE  ls.description IN ( 'WasteWater Fixed Charge',
                           'Waste Water Volumetric Charge',                         
                            'Water Consumption Charges');