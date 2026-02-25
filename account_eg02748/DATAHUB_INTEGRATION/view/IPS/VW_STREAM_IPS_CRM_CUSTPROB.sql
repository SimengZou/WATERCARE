CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CRM_CUSTPROB AS SELECT
                        src:ACCTKEY::integer AS ACCTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDBYBASEUSR::varchar AS ADDBYBASEUSR, 
                        src:ADDBYPORTALUSR::varchar AS ADDBYPORTALUSR, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:APKEY::integer AS APKEY, 
                        src:AREA::varchar AS AREA, 
                        src:ASGNFLAG::varchar AS ASGNFLAG, 
                        src:ASSETINSPFLAG::varchar AS ASSETINSPFLAG, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:CASEFLAG::varchar AS CASEFLAG, 
                        src:CLERICDTTM::datetime AS CLERICDTTM, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CUSTFLAG::varchar AS CUSTFLAG, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISTFROMFT::numeric(38, 10) AS DISTFROMFT, 
                        src:DISTFROMM::numeric(38, 10) AS DISTFROMM, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:DISTTOFT::numeric(38, 10) AS DISTTOFT, 
                        src:DISTTOM::numeric(38, 10) AS DISTTOM, 
                        src:EMAILNOTICE::varchar AS EMAILNOTICE, 
                        src:IMPACT::integer AS IMPACT, 
                        src:INCDTDTTM::datetime AS INCDTDTTM, 
                        src:INSPDAYS::integer AS INSPDAYS, 
                        src:INSPDTTM::datetime AS INSPDTTM, 
                        src:INSPECTR::varchar AS INSPECTR, 
                        src:INSPFLAG::varchar AS INSPFLAG, 
                        src:INSPHRS::integer AS INSPHRS, 
                        src:INSPMINS::integer AS INSPMINS, 
                        src:INWORKFLAG::varchar AS INWORKFLAG, 
                        src:LOC::varchar AS LOC, 
                        src:LOCNOTES::varchar AS LOCNOTES, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MOBILE::varchar AS MOBILE, 
                        src:MOBILEDTTM::datetime AS MOBILEDTTM, 
                        src:MOBILEEMP::varchar AS MOBILEEMP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODBYBASEUSR::varchar AS MODBYBASEUSR, 
                        src:MODBYPORTALUSR::varchar AS MODBYPORTALUSR, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MOREWORK::varchar AS MOREWORK, 
                        src:OOCFLAG::varchar AS OOCFLAG, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRI::varchar AS PRI, 
                        src:PROB::integer AS PROB, 
                        src:PROBDTTM::datetime AS PROBDTTM, 
                        src:PROJ::varchar AS PROJ, 
                        src:PROPKEY::integer AS PROPKEY, 
                        src:QTYCALLS::integer AS QTYCALLS, 
                        src:REFNO::varchar AS REFNO, 
                        src:RESCODE::varchar AS RESCODE, 
                        src:RESDTTM::datetime AS RESDTTM, 
                        src:RESDUEDTTM::datetime AS RESDUEDTTM, 
                        src:RESFLAG::varchar AS RESFLAG, 
                        src:RESP::varchar AS RESP, 
                        src:SCDCMPDTTM::datetime AS SCDCMPDTTM, 
                        src:SCHDUEDTTM::datetime AS SCHDUEDTTM, 
                        src:SCHEDDTTM::datetime AS SCHEDDTTM, 
                        src:SERVLINK::integer AS SERVLINK, 
                        src:SERVNO::integer AS SERVNO, 
                        src:SEVERITY::integer AS SEVERITY, 
                        src:SRC::varchar AS SRC, 
                        src:SRTYPE::varchar AS SRTYPE, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STDUEDTTM::datetime AS STDUEDTTM, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:SUSPDTTM::datetime AS SUSPDTTM, 
                        src:TAKENBY::varchar AS TAKENBY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WOFLAG::varchar AS WOFLAG, 
                        src:XCOORDINATE::numeric(38, 10) AS XCOORDINATE, 
                        src:YCOORDINATE::numeric(38, 10) AS YCOORDINATE, 
                        src:ZCOORDINATE::numeric(38, 10) AS ZCOORDINATE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:SERVNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CRM_CUSTPROB as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CLERICDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISTFROMFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISTFROMM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISTTOFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISTTOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPACT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INCDTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPHRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPMINS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROB), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PROBDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QTYCALLS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESDUEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCDCMPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCHDUEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVLINK), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEVERITY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STDUEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SUSPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null