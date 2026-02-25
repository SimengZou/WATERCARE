CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PORTAL_INVITATION AS SELECT
                        src:ACCEPTEDBY::integer AS ACCEPTEDBY, 
                        src:ACCEPTEDDATETIME::datetime AS ACCEPTEDDATETIME, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLICANTTYPE::integer AS APPLICANTTYPE, 
                        src:CANCELLEDBY::integer AS CANCELLEDBY, 
                        src:CANCELLEDDATETIME::datetime AS CANCELLEDDATETIME, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EXPIREDDATE::datetime AS EXPIREDDATE, 
                        src:INSTRUMENTNUMBER::varchar AS INSTRUMENTNUMBER, 
                        src:INVITATIONCODE::varchar AS INVITATIONCODE, 
                        src:INVITATIONKEY::integer AS INVITATIONKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRODUCTFAMILY::varchar AS PRODUCTFAMILY, 
                        src:REFERENCEINVITE::integer AS REFERENCEINVITE, 
                        src:SENDTO::varchar AS SENDTO, 
                        src:SENTBY::integer AS SENTBY, 
                        src:SENTDATETIME::datetime AS SENTDATETIME, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
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
    
                        
                src:INVITATIONKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:INVITATIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PORTAL_INVITATION as strm))