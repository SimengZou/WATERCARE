CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GROUPS AS SELECT
                        src:UGR_ADDPERM::varchar AS UGR_ADDPERM, 
                        src:UGR_CLASS::varchar AS UGR_CLASS, 
                        src:UGR_CLASS_ORG::varchar AS UGR_CLASS_ORG, 
                        src:UGR_CODE::varchar AS UGR_CODE, 
                        src:UGR_CORRBOOK::varchar AS UGR_CORRBOOK, 
                        src:UGR_CREATED::datetime AS UGR_CREATED, 
                        src:UGR_DESC::varchar AS UGR_DESC, 
                        src:UGR_DOWNLOADASSETINVENTORY::varchar AS UGR_DOWNLOADASSETINVENTORY, 
                        src:UGR_DOWNLOADBINS::varchar AS UGR_DOWNLOADBINS, 
                        src:UGR_DOWNLOADCOSTCODES::varchar AS UGR_DOWNLOADCOSTCODES, 
                        src:UGR_DOWNLOADDOCPRINTWO::varchar AS UGR_DOWNLOADDOCPRINTWO, 
                        src:UGR_DOWNLOADEMPLOYEE::varchar AS UGR_DOWNLOADEMPLOYEE, 
                        src:UGR_DOWNLOADEQCUSTOMFIELDS::varchar AS UGR_DOWNLOADEQCUSTOMFIELDS, 
                        src:UGR_DOWNLOADEQHISTCOMMENTS::varchar AS UGR_DOWNLOADEQHISTCOMMENTS, 
                        src:UGR_DOWNLOADEQUIPCOMMENTS::varchar AS UGR_DOWNLOADEQUIPCOMMENTS, 
                        src:UGR_DOWNLOADEQUIPHISTORY::varchar AS UGR_DOWNLOADEQUIPHISTORY, 
                        src:UGR_DOWNLOADEQUIPMENT::varchar AS UGR_DOWNLOADEQUIPMENT, 
                        src:UGR_DOWNLOADESIGNSETTINGS::varchar AS UGR_DOWNLOADESIGNSETTINGS, 
                        src:UGR_DOWNLOADINSPRESULTS::varchar AS UGR_DOWNLOADINSPRESULTS, 
                        src:UGR_DOWNLOADMTRREADING::varchar AS UGR_DOWNLOADMTRREADING, 
                        src:UGR_DOWNLOADPARTS::varchar AS UGR_DOWNLOADPARTS, 
                        src:UGR_DOWNLOADPHYINVENTORY::varchar AS UGR_DOWNLOADPHYINVENTORY, 
                        src:UGR_DOWNLOADSTANDARDWOS::varchar AS UGR_DOWNLOADSTANDARDWOS, 
                        src:UGR_DOWNLOADSTOCK::varchar AS UGR_DOWNLOADSTOCK, 
                        src:UGR_DOWNLOADSTORES::varchar AS UGR_DOWNLOADSTORES, 
                        src:UGR_DOWNLOADSUPPLIER::varchar AS UGR_DOWNLOADSUPPLIER, 
                        src:UGR_DOWNLOADTASK::varchar AS UGR_DOWNLOADTASK, 
                        src:UGR_DOWNLOADWORKORDERS::varchar AS UGR_DOWNLOADWORKORDERS, 
                        src:UGR_EQUIPFORDATASPY::numeric(38, 10) AS UGR_EQUIPFORDATASPY, 
                        src:UGR_FORBIN::varchar AS UGR_FORBIN, 
                        src:UGR_FORCATEGORY::varchar AS UGR_FORCATEGORY, 
                        src:UGR_FORCLASS_CC::varchar AS UGR_FORCLASS_CC, 
                        src:UGR_FORCLASS_EQUIP::varchar AS UGR_FORCLASS_EQUIP, 
                        src:UGR_FORCOSTCODE::varchar AS UGR_FORCOSTCODE, 
                        src:UGR_FORDATASPY::numeric(38, 10) AS UGR_FORDATASPY, 
                        src:UGR_FORDEPARTMENT_EMP::varchar AS UGR_FORDEPARTMENT_EMP, 
                        src:UGR_FORDEPARTMENT_EQUIP::varchar AS UGR_FORDEPARTMENT_EQUIP, 
                        src:UGR_FORDOWNLOADEDWO_EQCF::varchar AS UGR_FORDOWNLOADEDWO_EQCF, 
                        src:UGR_FORDOWNLOADEDWO_EQCMT::varchar AS UGR_FORDOWNLOADEDWO_EQCMT, 
                        src:UGR_FORDOWNLOADEDWO_EQHST::varchar AS UGR_FORDOWNLOADEDWO_EQHST, 
                        src:UGR_FOREMPLOYEE::varchar AS UGR_FOREMPLOYEE, 
                        src:UGR_FOREQUIPMENT::varchar AS UGR_FOREQUIPMENT, 
                        src:UGR_FORLATESTDAYS::numeric(38, 10) AS UGR_FORLATESTDAYS, 
                        src:UGR_FORLINE::varchar AS UGR_FORLINE, 
                        src:UGR_FORLOCATION::varchar AS UGR_FORLOCATION, 
                        src:UGR_FORPART_PART::varchar AS UGR_FORPART_PART, 
                        src:UGR_FORSTORE_BIN::varchar AS UGR_FORSTORE_BIN, 
                        src:UGR_FORSTORE_INV::varchar AS UGR_FORSTORE_INV, 
                        src:UGR_FORSTORE_PART::varchar AS UGR_FORSTORE_PART, 
                        src:UGR_FORTRADE::varchar AS UGR_FORTRADE, 
                        src:UGR_FORTYPE::varchar AS UGR_FORTYPE, 
                        src:UGR_JOBTYPE::varchar AS UGR_JOBTYPE, 
                        src:UGR_LASTSAVED::datetime AS UGR_LASTSAVED, 
                        src:UGR_LOGIN::varchar AS UGR_LOGIN, 
                        src:UGR_MAXINSTOCKANDQUANTITY::numeric(38, 10) AS UGR_MAXINSTOCKANDQUANTITY, 
                        src:UGR_MRC::varchar AS UGR_MRC, 
                        src:UGR_MTRREADINGLATESTDAYS::numeric(38, 10) AS UGR_MTRREADINGLATESTDAYS, 
                        src:UGR_PRINTER::varchar AS UGR_PRINTER, 
                        src:UGR_REQUESTOR::varchar AS UGR_REQUESTOR, 
                        src:UGR_SESSIONTIMEOUT::numeric(38, 10) AS UGR_SESSIONTIMEOUT, 
                        src:UGR_UPDATECOUNT::numeric(38, 10) AS UGR_UPDATECOUNT, 
                        src:UGR_UPDATED::datetime AS UGR_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UGR_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
                                        
                src:UGR_CODE  ORDER BY 
            src:UGR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GROUPS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UGR_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_EQUIPFORDATASPY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_FORDATASPY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_FORLATESTDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UGR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_MAXINSTOCKANDQUANTITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_MTRREADINGLATESTDAYS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_SESSIONTIMEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UGR_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UGR_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UGR_LASTSAVED), '1900-01-01')) is not null