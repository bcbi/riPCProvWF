-- Drop any existing temporary tables
DROP TABLE IF EXISTS #VACCINE_CODES;
DROP TABLE IF EXISTS #CORE_PRIMARY_CODES;
DROP TABLE IF EXISTS #LTC_CODES;
DROP TABLE IF EXISTS #HOME_CODES;
DROP TABLE IF EXISTS #CLAIM_SUMMARY_TABLE;
DROP TABLE IF EXISTS #PANEL_SIZES_AND_CLAIM_VOLUME;

DECLARE @APCD_MIN_CLAIM_COUNT INT = 10;
DECLARE @START_DATE DATE = '2022-06-01';
DECLARE @ONE_YEAR_BACK DATE = DATEADD(YEAR, -1, @START_DATE);

CREATE TABLE #VACCINE_CODES (ProcedureCode VARCHAR(5));
INSERT INTO #VACCINE_CODES (ProcedureCode) VALUES ('90471');

CREATE TABLE #CORE_PRIMARY_CODES (ProcedureCode VARCHAR(5));
INSERT INTO #CORE_PRIMARY_CODES (ProcedureCode)
VALUES ('G0402'), ('G0438'), ('G0439'), ('99381'), ('99382'), 
        ('99383'), ('99384'), ('99385'), ('99386'), ('99387'), 
        ('99391'), ('99392'), ('99393'), ('99394'), ('99395'), ('99396'), ('99397');

CREATE TABLE #LTC_CODES (ProcedureCode VARCHAR(5));
INSERT INTO #LTC_CODES (ProcedureCode)
VALUES ('99304'), ('99305'), ('99306'), ('99307'), ('99308'), ('99309'), ('99310');

CREATE TABLE #HOME_CODES (ProcedureCode VARCHAR(5));
INSERT INTO #HOME_CODES (ProcedureCode)
VALUES ('99341'), ('99342'), ('99343'), ('99344'), ('99345'), ('99346'), ('99347'), ('99348'), ('99349'), ('99350');

CREATE TABLE #CLAIM_SUMMARY_TABLE (
   Rendering_Provider_NPI BIGINT,
   Medical_Claim_Header_Id BIGINT,
   HAS_VACC_CODES BIT,
   HAS_PC_CODES BIT,
   HAS_LTC_CODES BIT,
   HAS_HOME_CODES BIT
);

INSERT INTO #CLAIM_SUMMARY_TABLE (
    Rendering_Provider_NPI, Medical_Claim_Header_Id, HAS_VACC_CODES, HAS_PC_CODES, HAS_LTC_CODES, HAS_HOME_CODES)
SELECT 
    mc.Rendering_Provider_NPI,
    mc.Medical_Claim_Header_Id,
    MAX(CASE WHEN vp.ProcedureCode IS NOT NULL THEN 1 ELSE 0 END) AS HAS_VACC_CODES,
    MAX(CASE WHEN cpc.ProcedureCode IS NOT NULL THEN 1 ELSE 0 END) AS HAS_PC_CODES,
    MAX(CASE WHEN ltc.ProcedureCode IS NOT NULL THEN 1 ELSE 0 END) AS HAS_LTC_CODES,
    MAX(CASE WHEN home.ProcedureCode IS NOT NULL THEN 1 ELSE 0 END) AS HAS_HOME_CODES
FROM 
    medical_claim mc
LEFT JOIN 
    #VACCINE_CODES vp ON mc.procedure_code = vp.ProcedureCode
LEFT JOIN 
    #CORE_PRIMARY_CODES cpc ON mc.procedure_code = cpc.ProcedureCode
LEFT JOIN 
    #LTC_CODES ltc ON mc.procedure_code = ltc.ProcedureCode
LEFT JOIN 
    #HOME_CODES home ON mc.procedure_code = home.ProcedureCode
WHERE 
    mc.procedure_code IN (
        SELECT ProcedureCode FROM #VACCINE_CODES
        UNION ALL
        SELECT ProcedureCode FROM #CORE_PRIMARY_CODES
        UNION ALL
        SELECT ProcedureCode FROM #LTC_CODES
        UNION ALL
        SELECT ProcedureCode FROM #HOME_CODES
    )
    AND mc.first_service_dt >= @START_DATE
    AND mc.Rendering_Provider_NPI <> ''
GROUP BY 
    mc.Rendering_Provider_NPI, 
    mc.Medical_Claim_Header_Id;




CREATE TABLE #PANEL_SIZES_AND_CLAIM_VOLUME (
   APCD_NPI BIGINT PRIMARY KEY,
   APCD_TOTAL_CLAIMS_ALL_COUNT BIGINT,
   APCD_MEMBER_ID_ALL_COUNT BIGINT,
   APCD_INTERNAL_MEMBER_ID_ALL_COUNT BIGINT,

   APCD_TOTAL_CLAIMS_ALL_TWO_YEAR_COUNT BIGINT,
   APCD_MEMBER_ID_ALL_TWO_YEAR_COUNT BIGINT,
   APCD_INTERNAL_MEMBER_ID_ALL_TWO_YEAR_COUNT BIGINT,

   APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT BIGINT,
   APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT BIGINT,
   APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT BIGINT,
   APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT BIGINT,

   APCD_MEMBER_ID_EXP_PC_ONE_YEAR_COUNT BIGINT,
   APCD_INTERNAL_MEMBER_ID_EXP_PC_ONE_YEAR_COUNT BIGINT,
   APCD_MEMBER_ID_EXP_PC_TWO_YEAR_COUNT BIGINT,
   APCD_INTERNAL_MEMBER_ID_EXP_PC_TWO_YEAR_COUNT BIGINT

);


-- Create pre-aggregated subqueries for each set of data
WITH Claims AS (
    SELECT
        Rendering_Provider_NPI,
        COUNT(DISTINCT Medical_Claim_Header_Id) AS Total_Claims_All,
        COUNT(DISTINCT Member_Id) AS Member_Id_All,
        COUNT(DISTINCT Internal_Member_Id) AS Internal_Member_Id_All
    FROM
        medical_claim
    WHERE
        first_service_dt >= @START_DATE
        AND Rendering_Provider_NPI <> ''
    GROUP BY
        Rendering_Provider_NPI
    HAVING
        COUNT(DISTINCT Medical_Claim_Header_Id) > @APCD_MIN_CLAIM_COUNT
),

TwoYearClaims AS (
    SELECT
        Rendering_Provider_NPI,
        COUNT(DISTINCT Medical_Claim_Header_Id) AS Total_Claims_All_Two_Year,
        COUNT(DISTINCT Member_Id) AS Member_Id_All_Two_Year,
        COUNT(DISTINCT Internal_Member_Id) AS Internal_Member_Id_All_Two_Year
    FROM
        medical_claim
    WHERE
        first_service_dt >= @ONE_YEAR_BACK
        AND Rendering_Provider_NPI <> ''
    GROUP BY
        Rendering_Provider_NPI
),

PCClaims AS (
    SELECT
        mc.Rendering_Provider_NPI,
        COUNT(DISTINCT mc.Member_Id) AS APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,
        COUNT(DISTINCT mc.Internal_Member_Id) AS APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT
    FROM
        medical_claim mc
    INNER JOIN (
        SELECT ProcedureCode FROM #VACCINE_CODES
        UNION ALL
        SELECT ProcedureCode FROM #CORE_PRIMARY_CODES
    ) AS pc_codes
    ON mc.procedure_code = pc_codes.ProcedureCode
    WHERE
        mc.first_service_dt >= @START_DATE
    GROUP BY
        mc.Rendering_Provider_NPI
),

TwoYearPCClaims AS (
    SELECT
        mc.Rendering_Provider_NPI,
        COUNT(DISTINCT mc.Member_Id) AS APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT,
        COUNT(DISTINCT mc.Internal_Member_Id) AS APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT
    FROM
        medical_claim mc
    INNER JOIN (
        SELECT ProcedureCode FROM #VACCINE_CODES
        UNION ALL
        SELECT ProcedureCode FROM #CORE_PRIMARY_CODES
    ) AS pc_codes
    ON mc.procedure_code = pc_codes.ProcedureCode
    WHERE
        mc.first_service_dt >= @ONE_YEAR_BACK
    GROUP BY
        mc.Rendering_Provider_NPI
)


INSERT INTO #PANEL_SIZES_AND_CLAIM_VOLUME (
    APCD_NPI,
    APCD_TOTAL_CLAIMS_ALL_COUNT,
    APCD_MEMBER_ID_ALL_COUNT,
    APCD_INTERNAL_MEMBER_ID_ALL_COUNT,
    APCD_TOTAL_CLAIMS_ALL_TWO_YEAR_COUNT,
    APCD_MEMBER_ID_ALL_TWO_YEAR_COUNT,
    APCD_INTERNAL_MEMBER_ID_ALL_TWO_YEAR_COUNT,
    APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,
    APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,
    APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT,
    APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT
)
SELECT
    c.Rendering_Provider_NPI AS APCD_NPI,
    c.Total_Claims_All AS APCD_TOTAL_CLAIMS_ALL_COUNT,
    CASE WHEN c.Member_Id_All > @APCD_MIN_CLAIM_COUNT THEN c.Member_Id_All ELSE 0 END AS APCD_MEMBER_ID_ALL_COUNT,
    CASE WHEN c.Internal_Member_Id_All > @APCD_MIN_CLAIM_COUNT THEN c.Internal_Member_Id_All ELSE 0 END AS APCD_INTERNAL_MEMBER_ID_ALL_COUNT,
    
    COALESCE(t.Total_Claims_All_Two_Year, 0) AS APCD_TOTAL_CLAIMS_ALL_TWO_YEAR_COUNT,
    COALESCE(CASE WHEN t.Member_Id_All_Two_Year > @APCD_MIN_CLAIM_COUNT THEN t.Member_Id_All_Two_Year ELSE 0 END, 0) AS APCD_MEMBER_ID_ALL_TWO_YEAR_COUNT,
    COALESCE(CASE WHEN t.Internal_Member_Id_All_Two_Year > @APCD_MIN_CLAIM_COUNT THEN t.Internal_Member_Id_All_Two_Year ELSE 0 END, 0) AS APCD_INTERNAL_MEMBER_ID_ALL_TWO_YEAR_COUNT,

    COALESCE(pc.APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT, 0) AS APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,
    COALESCE(pc.APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT, 0) AS APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,

    COALESCE(tpc.APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT, 0) AS APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT,
    COALESCE(tpc.APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT, 0) AS APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT
FROM
    Claims c
LEFT JOIN
    TwoYearClaims t ON c.Rendering_Provider_NPI = t.Rendering_Provider_NPI
LEFT JOIN
    PCClaims pc ON c.Rendering_Provider_NPI = pc.Rendering_Provider_NPI
LEFT JOIN
    TwoYearPCClaims tpc ON c.Rendering_Provider_NPI = tpc.Rendering_Provider_NPI;


SELECT 
    ps.APCD_NPI,
    ps.APCD_TOTAL_CLAIMS_ALL_COUNT,
    ps.APCD_MEMBER_ID_ALL_COUNT,
    ps.APCD_INTERNAL_MEMBER_ID_ALL_COUNT,
    ps.APCD_TOTAL_CLAIMS_ALL_TWO_YEAR_COUNT,
    ps.APCD_MEMBER_ID_ALL_TWO_YEAR_COUNT,
    ps.APCD_INTERNAL_MEMBER_ID_ALL_TWO_YEAR_COUNT,
    ps.APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,
    ps.APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT,
    ps.APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT,
    ps.APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT,
    cs.APCD_CORE_PC_CLAIMS_COUNT,
    cs.APCD_EXPANDED_PC_CLAIMS_COUNT,
    cs.APCD_VACC_CLAIMS_COUNT,
    cs.APCD_LTC_CLAIMS_COUNT,
    cs.APCD_HOME_CLAIMS_COUNT,
    cs.APCD_PC_CLAIMS_COUNT,
    cs.APCD_VACC_CLAIMS_PRESENT,
    cs.APCD_PC_CODES_PRESENT,
    cs.APCD_LTC_CODES_PRESENT,
    cs.APCD_HOME_CODES_PRESENT
FROM 
    #PANEL_SIZES_AND_CLAIM_VOLUME ps
JOIN 
    (
        SELECT 
            Rendering_Provider_NPI as APCD_NPI,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN HAS_VACC_CODES = 1 OR HAS_PC_CODES = 1 THEN Medical_Claim_Header_Id END) > @APCD_MIN_CLAIM_COUNT 
                THEN COUNT(DISTINCT CASE WHEN HAS_VACC_CODES = 1 OR HAS_PC_CODES = 1 THEN Medical_Claim_Header_Id END) 
                ELSE 0 
            END AS APCD_CORE_PC_CLAIMS_COUNT,
            CASE 
                WHEN COUNT(DISTINCT Medical_Claim_Header_Id) > @APCD_MIN_CLAIM_COUNT THEN COUNT(DISTINCT Medical_Claim_Header_Id) 
                ELSE 0 
            END AS APCD_EXPANDED_PC_CLAIMS_COUNT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN HAS_VACC_CODES = 1 THEN Medical_Claim_Header_Id END) > @APCD_MIN_CLAIM_COUNT 
                THEN COUNT(DISTINCT CASE WHEN HAS_VACC_CODES = 1 THEN Medical_Claim_Header_Id END) 
                ELSE 0 
            END AS APCD_VACC_CLAIMS_COUNT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN Has_LTC_CODES = 1 THEN Medical_Claim_Header_Id END) > @APCD_MIN_CLAIM_COUNT 
                THEN COUNT(DISTINCT CASE WHEN Has_LTC_CODES = 1 THEN Medical_Claim_Header_Id END) 
                ELSE 0 
            END AS APCD_LTC_CLAIMS_COUNT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN Has_HOME_CODES = 1 THEN Medical_Claim_Header_Id END) > @APCD_MIN_CLAIM_COUNT 
                THEN COUNT(DISTINCT CASE WHEN Has_HOME_CODES = 1 THEN Medical_Claim_Header_Id END) 
                ELSE 0 
            END AS APCD_HOME_CLAIMS_COUNT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN Has_PC_CODES = 1 THEN Medical_Claim_Header_Id END) > @APCD_MIN_CLAIM_COUNT 
                THEN COUNT(DISTINCT CASE WHEN Has_PC_CODES = 1 THEN Medical_Claim_Header_Id END) 
                ELSE 0 
            END AS APCD_PC_CLAIMS_COUNT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN HAS_VACC_CODES = 1 THEN Medical_Claim_Header_Id END) > 0 
                THEN 'True' 
                ELSE 'False' 
            END AS APCD_VACC_CLAIMS_PRESENT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN Has_PC_CODES = 1 THEN Medical_Claim_Header_Id END) > 0 
                THEN 'True' 
                ELSE 'False' 
            END AS APCD_PC_CODES_PRESENT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN Has_LTC_CODES = 1 THEN Medical_Claim_Header_Id END) > 0 
                THEN 'True' 
                ELSE 'False' 
            END AS APCD_LTC_CODES_PRESENT,
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN Has_HOME_CODES = 1 THEN Medical_Claim_Header_Id END) > 0 
                THEN 'True' 
                ELSE 'False' 
            END AS APCD_HOME_CODES_PRESENT
        FROM #CLAIM_SUMMARY_TABLE
        GROUP BY Rendering_Provider_NPI
        HAVING COUNT(DISTINCT Medical_Claim_Header_Id) > @APCD_MIN_CLAIM_COUNT
    ) cs
ON ps.APCD_NPI = cs.APCD_NPI
ORDER BY ps.APCD_NPI;