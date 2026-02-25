
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CLOSINGCODEHIERARCHY
                   as                       
                    SELECT
                        t.CCH_PARENTCLOSINGCODE,
                        t.CCH_PARENTCLOSINGCODETYPE,
                        t.CCH_CHILDCLOSINGCODE,
                        t.CCH_CHILDCLOSINGCODETYPE,
                        t.CCH_OBJECT,
                        t.CCH_OBJECT_ORG,
                        t.CCH_UPDATECOUNT,
                        t.CCH_LASTSAVED,
                        t.CCH_REPLDEPT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CLOSINGCODEHIERARCHY as  t
					 union
					        SELECT
                        th.CCH_PARENTCLOSINGCODE,
                        th.CCH_PARENTCLOSINGCODETYPE,
                        th.CCH_CHILDCLOSINGCODE,
                        th.CCH_CHILDCLOSINGCODETYPE,
                        th.CCH_OBJECT,
                        th.CCH_OBJECT_ORG,
                        th.CCH_UPDATECOUNT,
                        th.CCH_LASTSAVED,
                        th.CCH_REPLDEPT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CLOSINGCODEHIERARCHY as  th ;
                     