CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_DOCUMENTDETAILS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROVALDATE::varchar AS APPROVALDATE, 
                        src:CATEGORY::varchar AS CATEGORY, 
                        src:CT::varchar AS CT, 
                        src:CUSTOMERNAME::varchar AS CUSTOMERNAME, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DOCUMENTDETAILSKEY::integer AS DOCUMENTDETAILSKEY, 
                        src:DOCUMENTTYPE::varchar AS DOCUMENTTYPE, 
                        src:DP::varchar AS DP, 
                        src:EMAILKEYCOLUMN::varchar AS EMAILKEYCOLUMN, 
                        src:ENGINEERFULLNAME::varchar AS ENGINEERFULLNAME, 
                        src:EPANUMBER::varchar AS EPANUMBER, 
                        src:FROMEMAIL::varchar AS FROMEMAIL, 
                        src:JOBNAME::varchar AS JOBNAME, 
                        src:KEYCOLUMN::integer AS KEYCOLUMN, 
                        src:LOOKUPMONIKER::varchar AS LOOKUPMONIKER, 
                        src:LOOKUPSOURCE::varchar AS LOOKUPSOURCE, 
                        src:LOT::varchar AS LOT, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MONIKER::varchar AS MONIKER, 
                        src:NAME::varchar AS NAME, 
                        src:POSTCODE::varchar AS POSTCODE, 
                        src:REVIEWERDDI::varchar AS REVIEWERDDI, 
                        src:REVIEWEREMAIL::varchar AS REVIEWEREMAIL, 
                        src:REVIEWERNAME::varchar AS REVIEWERNAME, 
                        src:SOURCE::varchar AS SOURCE, 
                        src:STAGES::varchar AS STAGES, 
                        src:STREETNAME::varchar AS STREETNAME, 
                        src:STREETNUMBER::varchar AS STREETNUMBER, 
                        src:SUBJECT::varchar AS SUBJECT, 
                        src:SUBURB::varchar AS SUBURB, 
                        src:TOEMAIL::varchar AS TOEMAIL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VESTEDAMOUNT::numeric(38, 10) AS VESTEDAMOUNT, 
                        src:WASTEWATERCONNECTION::varchar AS WASTEWATERCONNECTION, 
                        src:WATERCAREREFNO::varchar AS WATERCAREREFNO, 
                        src:WATERCONNECTION::varchar AS WATERCONNECTION, 
                        src:WORKOVERAPPLICATIONNO::varchar AS WORKOVERAPPLICATIONNO, 
                        src:WORKSOVER::varchar AS WORKSOVER, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:DOCUMENTDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_DOCUMENTDETAILS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DOCUMENTDETAILSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:KEYCOLUMN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VESTEDAMOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null