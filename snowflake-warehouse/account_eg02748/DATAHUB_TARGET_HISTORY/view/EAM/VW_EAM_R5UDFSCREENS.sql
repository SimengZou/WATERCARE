
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5UDFSCREENS
                   as                       
                    SELECT
                        t.USC_SCREENNAME,
                        t.USC_DESC,
                        t.USC_TABLENAME,
                        t.USC_ISTAB,
                        t.USC_PARENTSCREEN,
                        t.USC_GENERATED,
                        t.USC_NOTUSED,
                        t.USC_CREATED,
                        t.USC_UPDATED,
                        t.USC_CREATEDBY,
                        t.USC_UPDATEDBY,
                        t.USC_UPDATECOUNT,
                        t.USC_ENTITY,
                        t.USC_ALLOWCOMMENTS,
                        t.USC_ALLOWDOCUMENTS,
                        t.USC_TYPEENTITY,
                        t.USC_STATUSENTITY,
                        t.USC_CLASS,
                        t.USC_ORGSECURITY,
                        t.USC_AUTOGENERATEKEY,
                        t.USC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5UDFSCREENS as  t
					 union
					        SELECT
                        th.USC_SCREENNAME,
                        th.USC_DESC,
                        th.USC_TABLENAME,
                        th.USC_ISTAB,
                        th.USC_PARENTSCREEN,
                        th.USC_GENERATED,
                        th.USC_NOTUSED,
                        th.USC_CREATED,
                        th.USC_UPDATED,
                        th.USC_CREATEDBY,
                        th.USC_UPDATEDBY,
                        th.USC_UPDATECOUNT,
                        th.USC_ENTITY,
                        th.USC_ALLOWCOMMENTS,
                        th.USC_ALLOWDOCUMENTS,
                        th.USC_TYPEENTITY,
                        th.USC_STATUSENTITY,
                        th.USC_CLASS,
                        th.USC_ORGSECURITY,
                        th.USC_AUTOGENERATEKEY,
                        th.USC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5UDFSCREENS as  th ;
                     