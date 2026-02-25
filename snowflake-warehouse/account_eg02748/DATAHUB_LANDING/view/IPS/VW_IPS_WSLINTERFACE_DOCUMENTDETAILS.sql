CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLINTERFACE_DOCUMENTDETAILS AS SELECT
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
                        src:WORKSOVER::varchar AS WORKSOVER, 
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
    
                        
                src:DOCUMENTDETAILSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DOCUMENTDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLINTERFACE_DOCUMENTDETAILS as strm))