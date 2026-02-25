CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5REPORTFILTERS AS SELECT
                        src:RFI_CHKBOX1::varchar AS RFI_CHKBOX1, 
                        src:RFI_CHKBOX10::varchar AS RFI_CHKBOX10, 
                        src:RFI_CHKBOX11::varchar AS RFI_CHKBOX11, 
                        src:RFI_CHKBOX12::varchar AS RFI_CHKBOX12, 
                        src:RFI_CHKBOX13::varchar AS RFI_CHKBOX13, 
                        src:RFI_CHKBOX14::varchar AS RFI_CHKBOX14, 
                        src:RFI_CHKBOX15::varchar AS RFI_CHKBOX15, 
                        src:RFI_CHKBOX16::varchar AS RFI_CHKBOX16, 
                        src:RFI_CHKBOX17::varchar AS RFI_CHKBOX17, 
                        src:RFI_CHKBOX18::varchar AS RFI_CHKBOX18, 
                        src:RFI_CHKBOX19::varchar AS RFI_CHKBOX19, 
                        src:RFI_CHKBOX2::varchar AS RFI_CHKBOX2, 
                        src:RFI_CHKBOX20::varchar AS RFI_CHKBOX20, 
                        src:RFI_CHKBOX21::varchar AS RFI_CHKBOX21, 
                        src:RFI_CHKBOX22::varchar AS RFI_CHKBOX22, 
                        src:RFI_CHKBOX23::varchar AS RFI_CHKBOX23, 
                        src:RFI_CHKBOX24::varchar AS RFI_CHKBOX24, 
                        src:RFI_CHKBOX25::varchar AS RFI_CHKBOX25, 
                        src:RFI_CHKBOX3::varchar AS RFI_CHKBOX3, 
                        src:RFI_CHKBOX4::varchar AS RFI_CHKBOX4, 
                        src:RFI_CHKBOX5::varchar AS RFI_CHKBOX5, 
                        src:RFI_CHKBOX6::varchar AS RFI_CHKBOX6, 
                        src:RFI_CHKBOX7::varchar AS RFI_CHKBOX7, 
                        src:RFI_CHKBOX8::varchar AS RFI_CHKBOX8, 
                        src:RFI_CHKBOX9::varchar AS RFI_CHKBOX9, 
                        src:RFI_DEFAULT::varchar AS RFI_DEFAULT, 
                        src:RFI_FROMDATE::datetime AS RFI_FROMDATE, 
                        src:RFI_FUNC::varchar AS RFI_FUNC, 
                        src:RFI_GROUPBY::varchar AS RFI_GROUPBY, 
                        src:RFI_LASTSAVED::datetime AS RFI_LASTSAVED, 
                        src:RFI_NAME::varchar AS RFI_NAME, 
                        src:RFI_ORDERBY::varchar AS RFI_ORDERBY, 
                        src:RFI_ORDERTYPE::varchar AS RFI_ORDERTYPE, 
                        src:RFI_ORG::varchar AS RFI_ORG, 
                        src:RFI_PARAM1::varchar AS RFI_PARAM1, 
                        src:RFI_PARAM10::varchar AS RFI_PARAM10, 
                        src:RFI_PARAM11::varchar AS RFI_PARAM11, 
                        src:RFI_PARAM12::varchar AS RFI_PARAM12, 
                        src:RFI_PARAM13::varchar AS RFI_PARAM13, 
                        src:RFI_PARAM14::varchar AS RFI_PARAM14, 
                        src:RFI_PARAM15::varchar AS RFI_PARAM15, 
                        src:RFI_PARAM16::varchar AS RFI_PARAM16, 
                        src:RFI_PARAM17::varchar AS RFI_PARAM17, 
                        src:RFI_PARAM18::varchar AS RFI_PARAM18, 
                        src:RFI_PARAM19::varchar AS RFI_PARAM19, 
                        src:RFI_PARAM2::varchar AS RFI_PARAM2, 
                        src:RFI_PARAM20::varchar AS RFI_PARAM20, 
                        src:RFI_PARAM21::varchar AS RFI_PARAM21, 
                        src:RFI_PARAM22::varchar AS RFI_PARAM22, 
                        src:RFI_PARAM23::varchar AS RFI_PARAM23, 
                        src:RFI_PARAM24::varchar AS RFI_PARAM24, 
                        src:RFI_PARAM25::varchar AS RFI_PARAM25, 
                        src:RFI_PARAM26::varchar AS RFI_PARAM26, 
                        src:RFI_PARAM27::varchar AS RFI_PARAM27, 
                        src:RFI_PARAM28::varchar AS RFI_PARAM28, 
                        src:RFI_PARAM29::varchar AS RFI_PARAM29, 
                        src:RFI_PARAM3::varchar AS RFI_PARAM3, 
                        src:RFI_PARAM30::varchar AS RFI_PARAM30, 
                        src:RFI_PARAM31::varchar AS RFI_PARAM31, 
                        src:RFI_PARAM32::varchar AS RFI_PARAM32, 
                        src:RFI_PARAM33::varchar AS RFI_PARAM33, 
                        src:RFI_PARAM34::varchar AS RFI_PARAM34, 
                        src:RFI_PARAM35::varchar AS RFI_PARAM35, 
                        src:RFI_PARAM36::varchar AS RFI_PARAM36, 
                        src:RFI_PARAM37::varchar AS RFI_PARAM37, 
                        src:RFI_PARAM38::varchar AS RFI_PARAM38, 
                        src:RFI_PARAM39::varchar AS RFI_PARAM39, 
                        src:RFI_PARAM4::varchar AS RFI_PARAM4, 
                        src:RFI_PARAM40::varchar AS RFI_PARAM40, 
                        src:RFI_PARAM5::varchar AS RFI_PARAM5, 
                        src:RFI_PARAM6::varchar AS RFI_PARAM6, 
                        src:RFI_PARAM7::varchar AS RFI_PARAM7, 
                        src:RFI_PARAM8::varchar AS RFI_PARAM8, 
                        src:RFI_PARAM9::varchar AS RFI_PARAM9, 
                        src:RFI_RADIO::varchar AS RFI_RADIO, 
                        src:RFI_TODATE::datetime AS RFI_TODATE, 
                        src:RFI_UPDATECOUNT::numeric(38, 10) AS RFI_UPDATECOUNT, 
                        src:RFI_UPDATED::datetime AS RFI_UPDATED, 
                        src:RFI_USER::varchar AS RFI_USER, 
                        src:RFI_VISFLDS::varchar AS RFI_VISFLDS, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:RFI_FUNC,
                src:RFI_NAME,
                src:RFI_USER,
            src:RFI_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RFI_FUNC,
                src:RFI_NAME,
                src:RFI_USER  ORDER BY 
            src:RFI_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5REPORTFILTERS as strm))