
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IESREPOSITORYCOLUMNS
                   as                       
                    SELECT
                        t.ENC_REPOSITORYID,
                        t.ENC_COLUMNNAME,
                        t.ENC_FIELDNAME,
                        t.ENC_ALIAS,
                        t.ENC_DATATYPE,
                        t.ENC_DISPLAYORDER,
                        t.ENC_PKORDER,
                        t.ENC_ISJSPKEYFIELD,
                        t.ENC_FACET,
                        t.ENC_UPDATECOUNT,
                        t.ENC_HIDDENFROMSEARCHRESULTS,
                        t.ENC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IESREPOSITORYCOLUMNS as  t
					 union
					        SELECT
                        th.ENC_REPOSITORYID,
                        th.ENC_COLUMNNAME,
                        th.ENC_FIELDNAME,
                        th.ENC_ALIAS,
                        th.ENC_DATATYPE,
                        th.ENC_DISPLAYORDER,
                        th.ENC_PKORDER,
                        th.ENC_ISJSPKEYFIELD,
                        th.ENC_FACET,
                        th.ENC_UPDATECOUNT,
                        th.ENC_HIDDENFROMSEARCHRESULTS,
                        th.ENC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IESREPOSITORYCOLUMNS as  th ;
                     