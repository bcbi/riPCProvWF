INPUT_FILES_DIRECTORY = "input_files"
RHODE_ISLAND_STATE_CODE = 'RI'
NAN_STRING = 'nan'

KNOWN_ORGANIZATIONAL_NPIS = {
    1013332014 : 'Roger Williams', # Only billing NPI was 1013332014, 1649278250 [both Roger Williams], or null
    1386643294 : 'Kent Hospital',  # Only billing NPI was 1386643294, 1023017944 [both Kent], or null
    1023049236 : 'The General Hospital Corporation', # Only billing NPI was 1023049236 or null
    1003362864 : 'Northwest Community Care', # Only billing NPI was 1003362864 or null
    1063574135 : 'BLACKSTONE VALLEY COMMUNITY HEALTH CARE, INC', # Only billing NPI was also 1063574135
    1013052976 : 'WOUND CARE GROUP', # Only billing NPI was 1013052976, 1578581443 [St Joseph health service], or 1871918870 [Our Lady Of Fatima Hospital]
    1003103391 : 'Rhode Island Hospital', # Only billing NPI was 1083848980 (RI outpt dialysis), 157881736 (UNKNOWN), 1053552299/1588659528 (RIH), 1083709109 (RIH psychiatric unit)
    1083848980 : 'RHODE ISLAND HOSPITAL OUTPT DIALYSIS',
    1134191836 : 'Minute Clinic',
    1154537520 : 'RIH Multi-specialty clinic',
    1164716031 : 'The Miriam Hospital',
    1184944662 : 'Affinity Physicians',
    1205056371 : 'DAY KIMBALL HEALTHCARE, INC.',
    1205832029 : 'Anchor Medical Associates',
    1225073638 : 'The Miriam Hospital',
    1245219070 : 'WELLONE PRIMARY MEDICAL AND DENTAL CARE', # Only billing NPI was also 1245219070
    1265422596 : 'Prima Care PC',
    1275810434 : 'The Miriam Hospital',
    1285191163 : 'Lifespan Physician Group', # Only billing NPI was 1285191163 or null
    1295818078 : 'EAST PROVIDENCE EMERGENCY ROOM INC',
    1306365812 : 'SILVER SPRING HEALTH CARE MANAGEMENT, INC',
    1306871223 : 'Charleton Memorial Hospital',
    1306896733 : 'Providence VAMC',
    1306954425 : 'Garden City Treatment Center',
    1336137629 : 'Southcoast Physician Services',
    1346218294 : 'Boston Medical Center',
    1366441719 : 'Comprehensive Community Action',
    1376891416 : 'Carewell Urgent Care',
    1386689149 : 'West Shore Health Center',
    1386821932 : 'MC Diagnostic of CT',
    1396780086 : 'TRI-COUNTY COMMUNITY ACTION AGENCY',
    1396849782 : 'Rhode Island Hospital',
    1417947383 : 'Wood River Health Services',
    1417959867 : 'Prairie Ave',
    1437119005 : 'Lincoln Urgent Care Center',
    1437628385 : 'Woonsocket Urgent Care Center',
    1447233788 : 'WIH',
    1447275037 : 'St Luke\'s',
    1447595368 : 'Westerly Hospital',
    1457346413 : 'Newport Hospital',
    1457470858 : 'MinuteClinic',
    1467517235 : 'The William Backus Hospital',
    1477527497 : 'MILFORD REGIONAL MEDICAL CENTER INC',
    1487203303 : 'WELLNESS CONNECT GO BEYOND MEDICINE, LLC',
    1487603585 : 'Cambridge Health Alliance',
    1487649430 : 'The Miriam Hospital',
    1508868035 : 'Central Health Center',
    1528221470 : 'NORTH PROVIDENCE URGENT CARE',
    1548202641 : 'Beth Israel Deaconness',
    1558392563 : 'Lahey Clinic Hospital',
    1558490151 : 'Atmed Treatment Center',
    1568555183 : 'SCMG EXPRESS CARE EAST GREENWICH',
    1578817136 : 'Rhode Island Hospital', # Only billing NPI was 1578817136 or null
    1588659528 : 'Rhode Island Hospital',
    1588691620 : 'Thundermist', # Only billing NPI was 1588691620 or null
    1598341067 : 'VMD PRIMARY PROVIDERS OF RHODE ISLAND PC',
    1598978744 : 'THE MIRIAM HOSPITAL PROFESSIONAL SERVICES',
    1609925122 : 'EAST BAY COMMUNITY ACTION PROGRAM',
    1629493135 : 'PROSPECT CHARTERCARE PHYSICIANS, LLC',
    1649511841 : 'MinuteClinic Diagnostic',
    1689626764 : 'Brown Medicine',
    1699752923 : 'Landmark Hospital',
    1700249851 : 'SILVER SPRING HEALTH CARE MANAGEMENT, INC.',
    1710087127 : 'Boston Children\'s Hospital',
    1710933072 : 'Wellness Company',
    1730106162 : 'Sturdy Hospital',
    1730132515 : 'Tufts Medical Center',
    1780273540 : 'Coastal Medical',
    1790717650 : 'Brigham and Women\'s',
    1821254525 : 'WELLONE PRIMARY MEDICAL AND DENTAL CARE',
    1831151455 : 'UMass Memorial',
    1851333686 : 'Dana-Farber',
    1861420333 : 'RIH',
    1861830143 : 'Southcoast Urgent Care',
    1871918870 : 'Our Lady Of Fatima Hospital',
    1891775128 : 'Providence Community Health Centers',
    1922185248 : 'Alert Ambulance Service',
    1932411493 : 'Steward St Annes',
    1952328569 : 'DEACTIVATED',
    1952366106 : 'South County Hospital',
    1972587699 : 'VISITING NURSE SERVICES OF NEWPORT AND BRISTOL COUNTIES',
    1982216974 : 'Rhode Island Health Group',
    1992737761 : 'NEWTON WELLESLEY HOSPITAL',
    1992857403 : 'ARMISTICE URGENT CARE & OCCUPATIONAL HEALTH, INC'
}




APCD_NPI_COL_NAME = 'APCD_NPI'
APCD_TOTAL_CLAIMS_ALL_COL_NAME = 'APCD_TOTAL_CLAIMS_ALL_COUNT'
APCD_PC_CODES_PRESENT_COL_NAME = 'APCD_PC_CODES_PRESENT'




ROLE_MISC_OTHER = 'Misc Other'
ROLE_PODIATRY = 'Podiatrist'
ROLE_OPTOMETRY = 'Optometrist'
ROLE_CASE_MGMT = 'Case Management'
ROLE_PSYCHOLOGIST = 'Psychologist'
ROLE_ORGANIZATION = 'Organization'
ROLE_CLIN_NURSE_SPECIALIST = 'Clinical Nurse Specialist'
ROLE_CERT_NURSE_MIDWIFE = 'Certified Nursing Midwife'
ROLE_NURSE = 'Nurse'
ROLE_STUDENT = 'Student'
ROLE_NP = 'Nurse Practitioner'
ROLE_PA = 'Physician Assistant' # older terminology used to match infrastructure used by RIDOH
ROLE_MD_DO = 'Physician'

RIDOH_PREFIX = 'RIDOH_'

SPECIALTY_EM = 'Emergency Medicine'
SPECIALTY_INTEG_MEDICINE = 'Integrative Medicine'
SPECIALTY_PREVENT_MEDICINE = 'Preventive Medicine'
SPECIALTY_INTERNAL_MEDICINE = 'Internal Medicine'
SPECIALTY_FAMILY_MEDICINE = 'Family Medicine'
SPECIALTY_GEN_PRACTICE = 'General Practice'
SPECIALTY_OBGYN = 'Obstetrics & Gynecology'
SPECIALTY_PEDS = 'Pediatrics'


NPPES_CLASSIFICATION = 'Classification' 
NPPES_CODE = 'Code'

DO_PREFIX = 'DO'
MD_PREFIX = 'MD'
CNM_PREFIX = 'CNM'
PA_PREFIX = 'PA'
APRN_PREFIX = 'APRN'
RN_PREFIX = 'RN'
ETL_PREFIX = 'ETL'
NPP_PREFIX = 'NPP'
CAPRN_PREFIX = 'CAPRN'
LP_PREFIX = 'LP'

UNCONFIRMED_STRING = 'Unconfirmed'