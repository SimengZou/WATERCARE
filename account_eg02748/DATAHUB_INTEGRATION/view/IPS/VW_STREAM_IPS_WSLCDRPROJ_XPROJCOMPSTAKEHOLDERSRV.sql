CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_XPROJCOMPSTAKEHOLDERSRV AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJREVDTLKEY::integer AS APPROJREVDTLKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTECITY::varchar AS WASTECITY, 
                        src:WASTECOMPANYNAME::varchar AS WASTECOMPANYNAME, 
                        src:WASTEEMAIL::varchar AS WASTEEMAIL, 
                        src:WASTEFIRSTNAME::varchar AS WASTEFIRSTNAME, 
                        src:WASTELASTNAME::varchar AS WASTELASTNAME, 
                        src:WASTEMOBILE::varchar AS WASTEMOBILE, 
                        src:WASTEPHONE::varchar AS WASTEPHONE, 
                        src:WASTEPOSTCODE::varchar AS WASTEPOSTCODE, 
                        src:WASTEREFERENCE::varchar AS WASTEREFERENCE, 
                        src:WASTESTREETNAME::varchar AS WASTESTREETNAME, 
                        src:WASTESTREETNO::varchar AS WASTESTREETNO, 
                        src:WASTESUBURB::varchar AS WASTESUBURB, 
                        src:WASTETITLE::varchar AS WASTETITLE, 
                        src:WATERCITY::varchar AS WATERCITY, 
                        src:WATERCOMPANYNAME::varchar AS WATERCOMPANYNAME, 
                        src:WATEREMAIL::varchar AS WATEREMAIL, 
                        src:WATERFIRSTNAME::varchar AS WATERFIRSTNAME, 
                        src:WATERLASTNAME::varchar AS WATERLASTNAME, 
                        src:WATERMOBILE::varchar AS WATERMOBILE, 
                        src:WATERPHONE::varchar AS WATERPHONE, 
                        src:WATERPOSTCODE::varchar AS WATERPOSTCODE, 
                        src:WATERREFERENCE::varchar AS WATERREFERENCE, 
                        src:WATERSTREETNAME::varchar AS WATERSTREETNAME, 
                        src:WATERSTREETNO::varchar AS WATERSTREETNO, 
                        src:WATERSUBURB::varchar AS WATERSUBURB, 
                        src:WATERTITLE::varchar AS WATERTITLE, 
                        src:XPROJCOMPSTAKEHOLDERSRVKEY::integer AS XPROJCOMPSTAKEHOLDERSRVKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XPROJCOMPSTAKEHOLDERSRVKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPROJ_XPROJCOMPSTAKEHOLDERSRV as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPROJREVDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XPROJCOMPSTAKEHOLDERSRVKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null