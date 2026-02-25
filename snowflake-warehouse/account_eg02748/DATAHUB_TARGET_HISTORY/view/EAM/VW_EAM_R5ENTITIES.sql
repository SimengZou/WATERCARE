
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ENTITIES
                   as                       
                    SELECT
                        t.ENT_ENTITY,
                        t.ENT_RENTITY,
                        t.ENT_STATENT,
                        t.ENT_TYPENT,
                        t.ENT_MULTILANG,
                        t.ENT_CLASSIF,
                        t.ENT_TABLE,
                        t.ENT_ADDATTRIBS,
                        t.ENT_FREETEXT,
                        t.ENT_ADDRESSES,
                        t.ENT_DOCUMENTS,
                        t.ENT_ASSPARTS,
                        t.ENT_ASSPERMITS,
                        t.ENT_FTAUDIT,
                        t.ENT_CAAUDIT,
                        t.ENT_ERECORD,
                        t.ENT_AUDITS,
                        t.ENT_UPDATECOUNT,
                        t.ENT_UPDATED,
                        t.ENT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ENTITIES as  t
					 union
					        SELECT
                        th.ENT_ENTITY,
                        th.ENT_RENTITY,
                        th.ENT_STATENT,
                        th.ENT_TYPENT,
                        th.ENT_MULTILANG,
                        th.ENT_CLASSIF,
                        th.ENT_TABLE,
                        th.ENT_ADDATTRIBS,
                        th.ENT_FREETEXT,
                        th.ENT_ADDRESSES,
                        th.ENT_DOCUMENTS,
                        th.ENT_ASSPARTS,
                        th.ENT_ASSPERMITS,
                        th.ENT_FTAUDIT,
                        th.ENT_CAAUDIT,
                        th.ENT_ERECORD,
                        th.ENT_AUDITS,
                        th.ENT_UPDATECOUNT,
                        th.ENT_UPDATED,
                        th.ENT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ENTITIES as  th ;
                     