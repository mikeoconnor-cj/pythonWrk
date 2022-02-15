USE Warehouse prod_adaugeopi;                                       
USE DATABASE prod_adaugeopi;                                       
                                       
WITH dupCTE
    AS
    (
    SELECT 'patient_x_chronic_condition_month' AS tableName
        , sum(rowsDuped) AS rowsEffected
        , count(*) AS pkRowCount
    FROM
        (
        SELECT chronic_condition_source_cd,
               fk_patient_id,
               measure_id,
               month_cd
            , count(*) AS rowsDuped 
        FROM INSIGHTS.patient_x_chronic_condition_month
        --WHERE CHRONIC_CONDITION_SOURCE_CD <> 'assgn_hcc' --all related to these
        GROUP BY chronic_condition_source_cd,
               fk_patient_id,
               measure_id,
               month_cd
        HAVING count(*) > 1
        ) a 
    ) 
    SELECT tableName 
        , 'prod_adaugeopi' AS orgDBName
        , rowsEffected
        , pkRowCount
    FROM dupCTE           
    