CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_BILL_ERROR AS SELECT src, 'IPS_BILLING_BILL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTUALAMT), '0'), 38, 10) is null and 
                    src:ACTUALAMT is not null) THEN
                    'ACTUALAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ACTUALAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJAMT), '0'), 38, 10) is null and 
                    src:ADJAMT is not null) THEN
                    'ADJAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ADJAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:ADJUSTMENTLINEITEMSAMOUNT is not null) THEN
                    'ADJUSTMENTLINEITEMSAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTLINEITEMSAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARNGAMT), '0'), 38, 10) is null and 
                    src:ARNGAMT is not null) THEN
                    'ARNGAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ARNGAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARNGPAYAMT), '0'), 38, 10) is null and 
                    src:ARNGPAYAMT is not null) THEN
                    'ARNGPAYAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ARNGPAYAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHIDENTIFIER), '0'), 38, 10) is null and 
                    src:BATCHIDENTIFIER is not null) THEN
                    'BATCHIDENTIFIER contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHIDENTIFIER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLADJAMT), '0'), 38, 10) is null and 
                    src:BILLADJAMT is not null) THEN
                    'BILLADJAMT contains a non-numeric value : \'' || AS_VARCHAR(src:BILLADJAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLAMT), '0'), 38, 10) is null and 
                    src:BILLAMT is not null) THEN
                    'BILLAMT contains a non-numeric value : \'' || AS_VARCHAR(src:BILLAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is null and 
                    src:BILLDATE is not null) THEN
                    'BILLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLPERFRDATE), '1900-01-01')) is null and 
                    src:BILLPERFRDATE is not null) THEN
                    'BILLPERFRDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLPERFRDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLPERTODATE), '1900-01-01')) is null and 
                    src:BILLPERTODATE is not null) THEN
                    'BILLPERTODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLPERTODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) THEN
                    'BILLRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETBILLINGPLANKEY), '0'), 38, 10) is null and 
                    src:BUDGETBILLINGPLANKEY is not null) THEN
                    'BUDGETBILLINGPLANKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BUDGETBILLINGPLANKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is null and 
                    src:CASSVER is not null) THEN
                    'CASSVER contains a non-numeric value : \'' || AS_VARCHAR(src:CASSVER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLAMT), '0'), 38, 10) is null and 
                    src:COLLAMT is not null) THEN
                    'COLLAMT contains a non-numeric value : \'' || AS_VARCHAR(src:COLLAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLKEY), '0'), 38, 10) is null and 
                    src:COLLKEY is not null) THEN
                    'COLLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COLLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONCCRED), '0'), 38, 10) is null and 
                    src:CONCCRED is not null) THEN
                    'CONCCRED contains a non-numeric value : \'' || AS_VARCHAR(src:CONCCRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRKEY), '0'), 38, 10) is null and 
                    src:CORRKEY is not null) THEN
                    'CORRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CORRKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATEDDTTM), '1900-01-01')) is null and 
                    src:CREATEDDTTM is not null) THEN
                    'CREATEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CREATEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUMVARIAMT), '0'), 38, 10) is null and 
                    src:CUMVARIAMT is not null) THEN
                    'CUMVARIAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CUMVARIAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRAMT), '0'), 38, 10) is null and 
                    src:CURRAMT is not null) THEN
                    'CURRAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CURRAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSRDS), '0'), 38, 10) is null and 
                    src:DAYSRDS is not null) THEN
                    'DAYSRDS contains a non-numeric value : \'' || AS_VARCHAR(src:DAYSRDS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFERREDPENALTYAMOUNT), '0'), 38, 10) is null and 
                    src:DEFERREDPENALTYAMOUNT is not null) THEN
                    'DEFERREDPENALTYAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DEFERREDPENALTYAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEFERREDPENALTYDATE), '1900-01-01')) is null and 
                    src:DEFERREDPENALTYDATE is not null) THEN
                    'DEFERREDPENALTYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DEFERREDPENALTYDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DELINQUENTDATE), '1900-01-01')) is null and 
                    src:DELINQUENTDATE is not null) THEN
                    'DELINQUENTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DELINQUENTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPAMT), '0'), 38, 10) is null and 
                    src:DEPAMT is not null) THEN
                    'DEPAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DEPAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPDUEAMT), '0'), 38, 10) is null and 
                    src:DEPDUEAMT is not null) THEN
                    'DEPDUEAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DEPDUEAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:DEPOSITLINEITEMSAMOUNT is not null) THEN
                    'DEPOSITLINEITEMSAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITLINEITEMSAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUNKEY is not null) THEN
                    'DIRECTDEBITRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCEXPAMT), '0'), 38, 10) is null and 
                    src:DISCEXPAMT is not null) THEN
                    'DISCEXPAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DISCEXPAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISCEXPDATE), '1900-01-01')) is null and 
                    src:DISCEXPDATE is not null) THEN
                    'DISCEXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISCEXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:DISCOUNTLINEITEMSAMOUNT is not null) THEN
                    'DISCOUNTLINEITEMSAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DISCOUNTLINEITEMSAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPAMT), '0'), 38, 10) is null and 
                    src:DISPAMT is not null) THEN
                    'DISPAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DISPAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DLNQAMT), '0'), 38, 10) is null and 
                    src:DLNQAMT is not null) THEN
                    'DLNQAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DLNQAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) THEN
                    'DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DUEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is null and 
                    src:EXTRACTDATE is not null) THEN
                    'EXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSTNO), '0'), 38, 10) is null and 
                    src:INSTNO is not null) THEN
                    'INSTNO contains a non-numeric value : \'' || AS_VARCHAR(src:INSTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSTTOTAMT), '0'), 38, 10) is null and 
                    src:INSTTOTAMT is not null) THEN
                    'INSTTOTAMT contains a non-numeric value : \'' || AS_VARCHAR(src:INSTTOTAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTAMT), '0'), 38, 10) is null and 
                    src:INTAMT is not null) THEN
                    'INTAMT contains a non-numeric value : \'' || AS_VARCHAR(src:INTAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LIENAMT), '0'), 38, 10) is null and 
                    src:LIENAMT is not null) THEN
                    'LIENAMT contains a non-numeric value : \'' || AS_VARCHAR(src:LIENAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LIENKEY), '0'), 38, 10) is null and 
                    src:LIENKEY is not null) THEN
                    'LIENKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LIENKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OVERAMT), '0'), 38, 10) is null and 
                    src:OVERAMT is not null) THEN
                    'OVERAMT contains a non-numeric value : \'' || AS_VARCHAR(src:OVERAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAIDDTTM), '1900-01-01')) is null and 
                    src:PAIDDTTM is not null) THEN
                    'PAIDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:PAIDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTLINEITEMSAMOUNT is not null) THEN
                    'PAYMENTLINEITEMSAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTLINEITEMSAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORDER), '0'), 38, 10) is null and 
                    src:PAYORDER is not null) THEN
                    'PAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:PENALTYLINEITEMSAMOUNT is not null) THEN
                    'PENALTYLINEITEMSAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTYLINEITEMSAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENAMT), '0'), 38, 10) is null and 
                    src:PENAMT is not null) THEN
                    'PENAMT contains a non-numeric value : \'' || AS_VARCHAR(src:PENAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:POSTEDTTM), '1900-01-01')) is null and 
                    src:POSTEDTTM is not null) THEN
                    'POSTEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:POSTEDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREVIOUSBILLKEY), '0'), 38, 10) is null and 
                    src:PREVIOUSBILLKEY is not null) THEN
                    'PREVIOUSBILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PREVIOUSBILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALTOTALAMOUNT), '0'), 38, 10) is null and 
                    src:PRINCIPALTOTALAMOUNT is not null) THEN
                    'PRINCIPALTOTALAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PRINCIPALTOTALAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIORBILLAMT), '0'), 38, 10) is null and 
                    src:PRIORBILLAMT is not null) THEN
                    'PRIORBILLAMT contains a non-numeric value : \'' || AS_VARCHAR(src:PRIORBILLAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEDBILLKEY), '0'), 38, 10) is null and 
                    src:REVERSEDBILLKEY is not null) THEN
                    'REVERSEDBILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSEDBILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETLAMT), '0'), 38, 10) is null and 
                    src:SETLAMT is not null) THEN
                    'SETLAMT contains a non-numeric value : \'' || AS_VARCHAR(src:SETLAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETTLEMENTKEY), '0'), 38, 10) is null and 
                    src:SETTLEMENTKEY is not null) THEN
                    'SETTLEMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SETTLEMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIAMT), '0'), 38, 10) is null and 
                    src:VARIAMT is not null) THEN
                    'VARIAMT contains a non-numeric value : \'' || AS_VARCHAR(src:VARIAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
                etl_load_datetime,
                etl_load_metadatafilename
                FROM 
                (
                select 
                    src,
                    etl_load_datetime,
                    etl_load_metadatafilename
                    from
                    (
                        SELECT
        
                            
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:BILLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTUALAMT), '0'), 38, 10) is null and 
                    src:ACTUALAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJAMT), '0'), 38, 10) is null and 
                    src:ADJAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:ADJUSTMENTLINEITEMSAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARNGAMT), '0'), 38, 10) is null and 
                    src:ARNGAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARNGPAYAMT), '0'), 38, 10) is null and 
                    src:ARNGPAYAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHIDENTIFIER), '0'), 38, 10) is null and 
                    src:BATCHIDENTIFIER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLADJAMT), '0'), 38, 10) is null and 
                    src:BILLADJAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLAMT), '0'), 38, 10) is null and 
                    src:BILLAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is null and 
                    src:BILLDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLPERFRDATE), '1900-01-01')) is null and 
                    src:BILLPERFRDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLPERTODATE), '1900-01-01')) is null and 
                    src:BILLPERTODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETBILLINGPLANKEY), '0'), 38, 10) is null and 
                    src:BUDGETBILLINGPLANKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is null and 
                    src:CASSVER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLAMT), '0'), 38, 10) is null and 
                    src:COLLAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COLLKEY), '0'), 38, 10) is null and 
                    src:COLLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONCCRED), '0'), 38, 10) is null and 
                    src:CONCCRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRKEY), '0'), 38, 10) is null and 
                    src:CORRKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATEDDTTM), '1900-01-01')) is null and 
                    src:CREATEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUMVARIAMT), '0'), 38, 10) is null and 
                    src:CUMVARIAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRAMT), '0'), 38, 10) is null and 
                    src:CURRAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSRDS), '0'), 38, 10) is null and 
                    src:DAYSRDS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFERREDPENALTYAMOUNT), '0'), 38, 10) is null and 
                    src:DEFERREDPENALTYAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEFERREDPENALTYDATE), '1900-01-01')) is null and 
                    src:DEFERREDPENALTYDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DELINQUENTDATE), '1900-01-01')) is null and 
                    src:DELINQUENTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPAMT), '0'), 38, 10) is null and 
                    src:DEPAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPDUEAMT), '0'), 38, 10) is null and 
                    src:DEPDUEAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:DEPOSITLINEITEMSAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCEXPAMT), '0'), 38, 10) is null and 
                    src:DISCEXPAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISCEXPDATE), '1900-01-01')) is null and 
                    src:DISCEXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:DISCOUNTLINEITEMSAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPAMT), '0'), 38, 10) is null and 
                    src:DISPAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DLNQAMT), '0'), 38, 10) is null and 
                    src:DLNQAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is null and 
                    src:EXTRACTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSTNO), '0'), 38, 10) is null and 
                    src:INSTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSTTOTAMT), '0'), 38, 10) is null and 
                    src:INSTTOTAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTAMT), '0'), 38, 10) is null and 
                    src:INTAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LIENAMT), '0'), 38, 10) is null and 
                    src:LIENAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LIENKEY), '0'), 38, 10) is null and 
                    src:LIENKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OVERAMT), '0'), 38, 10) is null and 
                    src:OVERAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PAIDDTTM), '1900-01-01')) is null and 
                    src:PAIDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTLINEITEMSAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORDER), '0'), 38, 10) is null and 
                    src:PAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYLINEITEMSAMOUNT), '0'), 38, 10) is null and 
                    src:PENALTYLINEITEMSAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENAMT), '0'), 38, 10) is null and 
                    src:PENAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:POSTEDTTM), '1900-01-01')) is null and 
                    src:POSTEDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREVIOUSBILLKEY), '0'), 38, 10) is null and 
                    src:PREVIOUSBILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALTOTALAMOUNT), '0'), 38, 10) is null and 
                    src:PRINCIPALTOTALAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIORBILLAMT), '0'), 38, 10) is null and 
                    src:PRIORBILLAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEDBILLKEY), '0'), 38, 10) is null and 
                    src:REVERSEDBILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETLAMT), '0'), 38, 10) is null and 
                    src:SETLAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETTLEMENTKEY), '0'), 38, 10) is null and 
                    src:SETTLEMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIAMT), '0'), 38, 10) is null and 
                    src:VARIAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)