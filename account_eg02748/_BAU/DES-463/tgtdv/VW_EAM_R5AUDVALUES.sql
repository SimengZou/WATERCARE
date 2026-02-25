
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5AUDVALUES
                   as                       
                    SELECT
                        t.AVA_ATTRIBUTE,
                        t.AVA_TABLE,
                        t.AVA_PRIMARYID,
                        t.AVA_SECONDARYID,
                        t.AVA_FROM,
                        t.AVA_TO,
                        t.AVA_CHANGED,
                        t.AVA_MODIFIEDBY,
                        t.AVA_FUNCTION,
                        t.AVA_UPDATED,
                        t.AVA_INSERTED,
                        t.AVA_DELETED,
                        t.AVA_UPDATECOUNT,
                        t.AVA_SCODE,
                        t.AVA_MOBILE,
                        t.AVA_PRIMARYID2,
                        t.AVA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5AUDVALUES as  t
					 union
					        SELECT
                        th.AVA_ATTRIBUTE,
                        th.AVA_TABLE,
                        th.AVA_PRIMARYID,
                        th.AVA_SECONDARYID,
                        th.AVA_FROM,
                        th.AVA_TO,
                        th.AVA_CHANGED,
                        th.AVA_MODIFIEDBY,
                        th.AVA_FUNCTION,
                        th.AVA_UPDATED,
                        th.AVA_INSERTED,
                        th.AVA_DELETED,
                        th.AVA_UPDATECOUNT,
                        th.AVA_SCODE,
                        th.AVA_MOBILE,
                        th.AVA_PRIMARYID2,
                        th.AVA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5AUDVALUES as  th ;
                     