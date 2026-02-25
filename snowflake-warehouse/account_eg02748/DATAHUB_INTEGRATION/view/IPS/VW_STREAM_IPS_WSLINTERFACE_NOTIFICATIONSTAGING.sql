CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NOTIFICATIONSTAGING AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLICATIONNUMBER::varchar AS APPLICATIONNUMBER, 
                        src:APPLICATIONTYPE::varchar AS APPLICATIONTYPE, 
                        src:CATEGORY::varchar AS CATEGORY, 
                        src:CONTACTEMAILADDRESS::varchar AS CONTACTEMAILADDRESS, 
                        src:CONTACTFULLNAME::varchar AS CONTACTFULLNAME, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DOCUMENTID::varchar AS DOCUMENTID, 
                        src:EPA::varchar AS EPA, 
                        src:FROMEMAIL::varchar AS FROMEMAIL, 
                        src:KEYCOLUMN::integer AS KEYCOLUMN, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MONIKER::varchar AS MONIKER, 
                        src:NAME::varchar AS NAME, 
                        src:NOTIFICATIONSTAGINGKEY::integer AS NOTIFICATIONSTAGINGKEY, 
                        src:R4CURL::varchar AS R4CURL, 
                        src:REVIEWASSIGNDAYPHONE::varchar AS REVIEWASSIGNDAYPHONE, 
                        src:REVIEWASSIGNEMAIL::varchar AS REVIEWASSIGNEMAIL, 
                        src:REVIEWASSIGNFULLNAME::varchar AS REVIEWASSIGNFULLNAME, 
                        src:REVIEWCOMPLETEDAYPHONE::varchar AS REVIEWCOMPLETEDAYPHONE, 
                        src:REVIEWCOMPLETEEMAIL::varchar AS REVIEWCOMPLETEEMAIL, 
                        src:REVIEWCOMPLETEFULLNAME::varchar AS REVIEWCOMPLETEFULLNAME, 
                        src:REVIEWERROLE::varchar AS REVIEWERROLE, 
                        src:SOURCE::varchar AS SOURCE, 
                        src:SUBJECT::varchar AS SUBJECT, 
                        src:TEMPLATENAME::varchar AS TEMPLATENAME, 
                        src:TITLE::varchar AS TITLE, 
                        src:TOEMAIL::varchar AS TOEMAIL, 
                        src:USERTYPE::varchar AS USERTYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                                        
                src:NOTIFICATIONSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_NOTIFICATIONSTAGING as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KEYCOLUMN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOTIFICATIONSTAGINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null