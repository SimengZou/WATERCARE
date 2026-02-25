
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LOCALE
                   as                       
                    SELECT
                        t.LOC_CODE,
                        t.LOC_DESC,
                        t.LOC_DECSYM,
                        t.LOC_MONDECSYM,
                        t.LOC_GRPSYM,
                        t.LOC_GRPDIGITS,
                        t.LOC_FRACTDIGITS,
                        t.LOC_MONFRACT,
                        t.LOC_NEGSYM,
                        t.LOC_POSSYM,
                        t.LOC_DATEFMT,
                        t.LOC_STARTDAY,
                        t.LOC_UPDATECOUNT,
                        t.LOC_MONTGRPSEPARATOR,
                        t.LOC_MONTGRPDIGITS,
                        t.LOC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LOCALE as  t
					 union
					        SELECT
                        th.LOC_CODE,
                        th.LOC_DESC,
                        th.LOC_DECSYM,
                        th.LOC_MONDECSYM,
                        th.LOC_GRPSYM,
                        th.LOC_GRPDIGITS,
                        th.LOC_FRACTDIGITS,
                        th.LOC_MONFRACT,
                        th.LOC_NEGSYM,
                        th.LOC_POSSYM,
                        th.LOC_DATEFMT,
                        th.LOC_STARTDAY,
                        th.LOC_UPDATECOUNT,
                        th.LOC_MONTGRPSEPARATOR,
                        th.LOC_MONTGRPDIGITS,
                        th.LOC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LOCALE as  th ;
                     