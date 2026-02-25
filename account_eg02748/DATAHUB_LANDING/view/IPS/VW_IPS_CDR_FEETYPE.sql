CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_FEETYPE AS SELECT
                        src:ACCTPREFIX::varchar AS ACCTPREFIX, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ATCOSTUSEDFOR::integer AS ATCOSTUSEDFOR, 
                        src:AUTOPAY::varchar AS AUTOPAY, 
                        src:CDRPRODUCTFAMILY::integer AS CDRPRODUCTFAMILY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESTBGTNOKEY::integer AS DESTBGTNOKEY, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FEECLASS::integer AS FEECLASS, 
                        src:FEECODE::varchar AS FEECODE, 
                        src:FEEDESC::varchar AS FEEDESC, 
                        src:FEEGROUP::varchar AS FEEGROUP, 
                        src:FEETYPEKEY::integer AS FEETYPEKEY, 
                        src:INITIALDEPFEETYPEKEY::integer AS INITIALDEPFEETYPEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORDERING::integer AS ORDERING, 
                        src:PAYMENTMETHODFLAG::integer AS PAYMENTMETHODFLAG, 
                        src:PAYORD::integer AS PAYORD, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:REFUNDABLE::varchar AS REFUNDABLE, 
                        src:SRCBGTNOKEY::integer AS SRCBGTNOKEY, 
                        src:SURC::varchar AS SURC, 
                        src:USEINJOBESTIMATION::varchar AS USEINJOBESTIMATION, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VERSIONDATEBIND::varchar AS VERSIONDATEBIND, 
                        src:VERSIONDATEMONIKER::varchar AS VERSIONDATEMONIKER, 
                        src:WAIVABLE::varchar AS WAIVABLE, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:FEETYPEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FEETYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_FEETYPE as strm))