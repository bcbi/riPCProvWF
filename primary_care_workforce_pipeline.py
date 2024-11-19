# %% [markdown]
# # Dataset Generation # 
# 
# ## Obtaining base data from APCD ##
# ### Data from APCD must be obtained by logging following these steps:
# 
# <ul>
# <li>Log into Stronghold (Brown University Secure Computing Environment) from APCD data can be accessed</li>
# <li>Run dbeaver from command line to access the DBeaver SQL studio UI</li>
# <li>Run the code in /apcd_sql_files/APCD_Data_Extract.sql and save the result in a file called apcd_data_extract.csv in the input_files folder </li>
# </ul>
# 
# ### Python Libraries / Constants Necessary For Subsequent Logic ###
# 

# %%
from pc_utilities import *
from pc_constants import *

# %% [markdown]
# ### Creating a single dataframe from APCD Data 

# %%
import pandas as pd

apcd_file_name = 'apcd_data_extract.csv'
apcd_provider_data = import_csv_gracefully(INPUT_FILES_DIRECTORY, apcd_file_name)
print("Importing data from APCD yielded:", len(apcd_provider_data[APCD_NPI_COL_NAME].unique()), "unique NPIs.")

apcd_provider_data = apcd_provider_data[~apcd_provider_data[APCD_NPI_COL_NAME].isin(list(KNOWN_ORGANIZATIONAL_NPIS.keys()))]
unique_APCD_npis = apcd_provider_data[APCD_NPI_COL_NAME].unique()
print("Post dropping known organizational NPIs:", len(unique_APCD_npis), "unique NPIs.")

# %% [markdown]
# #### Generating RIDOH Files
# 
# ##### Sanity Check: Reconciling NPPES with RIDOH License Database
# 
# As a sanity check to ensure providers aren't missed, the NPPES database should be reconciled with the RIDOH License Database.
# 
# ##### Instructions for Downloading Relevant Files:
# 
# 1. Visit [Rhode Island Department of Health - Licensee Lists](https://health.ri.gov/lists/licensees/).
# 2. Download the appropriate files by **Profession or Facility**, selecting:
#    - "Physician"
#    - "Physician Assistant"
#    - "Nursing"
#    - "Midwifery"
# 3. For each file, place it in the input_files folder!

# %%
# Update the below! 
ridoh_physician_licensee_extract_file_name = 'Physician-licensee-extract-2024-10-07.csv'
ridoh_physician_assistant_licensee_extract_file_name = 'Physician-Assistant-licensee-extract-2024-10-07.csv'
ridoh_nursing_licensee_extract_file_name = 'Nursing-licensee-extract-2024-10-07.csv'
ridoh_midwife_licensee_extract_file_name = 'Midwifery-licensee-extract-2024-10-07.csv'


# Nothing to update below! 
RIDOH_FIRST_NAME_COL_NAME = add_source_db_prefix('First', RIDOH_PREFIX)
RIDOH_LAST_NAME_COL_NAME = add_source_db_prefix('Last', RIDOH_PREFIX)
RIDOH_LICENSE_NO_COL_NAME = add_source_db_prefix('License No', RIDOH_PREFIX) 
RIDOH_CREDENTIAL_COLUMN_NAME = add_source_db_prefix('Credential', RIDOH_PREFIX) 
SPECIALTY_COLUMN_NAME = add_source_db_prefix('Specialty', RIDOH_PREFIX) 

RIDOH_NAME_COL_NAME = add_source_db_prefix('Name', RIDOH_PREFIX)
RIDOH_MIDDLE_COL_NAME = add_source_db_prefix('Middle', RIDOH_PREFIX)
RIDOH_LIC_TYPE_COL_NAME = add_source_db_prefix('License Type', RIDOH_PREFIX)
RIDOH_STATUS_COL_NAME = add_source_db_prefix('Status', RIDOH_PREFIX)
RIDOH_ISSUE_DATE_COL_NAME = add_source_db_prefix('Issue Date', RIDOH_PREFIX)
RIDOH_EXP_DATE_COL_NAME = add_source_db_prefix('Expiration Date', RIDOH_PREFIX)
RIDOH_ADDRESS_ONE_COL_NAME = add_source_db_prefix('Address Line 1', RIDOH_PREFIX)
RIDOH_ADDRESS_TWO_COL_NAME = add_source_db_prefix('Address Line 2', RIDOH_PREFIX)
RIDOH_ADDRESS_THREE_COL_NAME = add_source_db_prefix('Address Line 3', RIDOH_PREFIX)
RIDOH_CITY_COL_NAME = add_source_db_prefix('City', RIDOH_PREFIX)
RIDOH_STATE_COL_NAME = add_source_db_prefix('State', RIDOH_PREFIX)
RIDOH_ZIP_COL_NAME = add_source_db_prefix('Zip', RIDOH_PREFIX)
RIDOH_EMAIL_COL_NAME = add_source_db_prefix('Email', RIDOH_PREFIX)
RIDOH_PHONE_COL_NAME = add_source_db_prefix('Phone', RIDOH_PREFIX)
RIDOH_FAX_COL_NAME = add_source_db_prefix('Fax', RIDOH_PREFIX)
RIDOH_PROF_COL_NAME = add_source_db_prefix('Profession', RIDOH_PREFIX)

ridoh_physicians = import_csv_gracefully(INPUT_FILES_DIRECTORY, ridoh_physician_licensee_extract_file_name)
ridoh_physicians[RIDOH_CREDENTIAL_COLUMN_NAME] = ROLE_MD_DO

ridoh_physicians_assistant = import_csv_gracefully(INPUT_FILES_DIRECTORY,ridoh_physician_assistant_licensee_extract_file_name)
ridoh_physicians_assistant[RIDOH_CREDENTIAL_COLUMN_NAME] = ROLE_PA

ridoh_midwifery = import_csv_gracefully(INPUT_FILES_DIRECTORY, ridoh_midwife_licensee_extract_file_name)
ridoh_midwifery[RIDOH_CREDENTIAL_COLUMN_NAME] = ROLE_CERT_NURSE_MIDWIFE

# Includes both nurses and NPs so won't set credential for now (may use license type later for this analysis)
ridoh_nursing = import_csv_gracefully(INPUT_FILES_DIRECTORY, ridoh_nursing_licensee_extract_file_name)

ridoh_clinicians = pd.concat([ridoh_physicians, ridoh_physicians_assistant, ridoh_nursing, ridoh_midwifery], ignore_index=True)
excluded_column = ridoh_clinicians[[RIDOH_CREDENTIAL_COLUMN_NAME]]
remaining_columns = ridoh_clinicians.drop(columns=[RIDOH_CREDENTIAL_COLUMN_NAME]).add_prefix(RIDOH_PREFIX)
ridoh_clinicians = pd.concat([excluded_column, remaining_columns], axis=1)

ridoh_names_of_interest = list(ridoh_clinicians[[RIDOH_FIRST_NAME_COL_NAME, RIDOH_LAST_NAME_COL_NAME]].itertuples(index=False, name=None))
ridoh_licenses = ridoh_clinicians[RIDOH_LICENSE_NO_COL_NAME]

ridoh_first_names = [pair[0] for pair in ridoh_names_of_interest]
ridoh_last_names = [pair[1] for pair in ridoh_names_of_interest]

# %% [markdown]
# ### Generating NPPES Files
# 
# #### Accessing the NPPES Database
# 
# - The NPPES database can be found on the CMS [webpage](https://www.cms.gov/medicare/regulations-guidance/administrative-simplification/data-dissemination).
# - The actual file you need is the **Data Dissemination** file, which can be downloaded [here](https://download.cms.gov/nppes/NPI_Files.html).
# - Look for the relevant CSV file that starts with `npidata_pfile` (e.g., `npidata_pfile_20050523-20240107`) and place it in the input_files folder!
# 
# ### Filtering NPPES
# 
# NPPES was the source of truth on provider metadata. We went through the entire database and then:
# - Only included those NPIs that we had seen in our APCD queries - NPIs that didn't show up in APCD are presumed to not have billed RI payors and therefore not be under consideration
# - For those NPIs that were seen in the APCD queries, we still need to confirm that the providers are Rhode Island based. We did this by checking against three sources:
#     - Did the provider have an address associated with RI in terms of personal or business address?
#     - Did the provider have a license number that matched the RIDOH license database?
#     - Did the provider have a name that matched the RIDOH license database? 
# 
# Of note, the public-facing RIDOH database did not include NPI numbers so this version of the logic checked against name and license number only. Future versions should consider obtaining the NPI number.
# 
# Current runtime for this step is approximately 10 minutes.

# %%
# Update the below! 
nppes_provider_data_file = 'npidata_pfile_20050523-20240107.csv'
output_ri_nppes_clinicians_file_suffix = '_ri_clinicians.csv'
# This flag indicates whether the NPPES data set should explicitly exclude any NPI associated with entity type=2 or organization
# Default is to not exclude to be conservative! 
exclude_organizations = False




# Nothing to update below! 
# chunk_size can be modified depending on the memory/performance of the machine on which the code is run!

NPPES_PREFIX = 'NPPES_'

# Looking for providers in NPPES with a RI connection based on any noted state being RI
address_columns_to_check = [
    add_source_db_prefix('Provider Business Mailing Address State Name', NPPES_PREFIX),
    add_source_db_prefix('Provider Business Practice Location Address State Name', NPPES_PREFIX)
]

license_columns = [f'{NPPES_PREFIX}Provider License Number State Code_{i}' for i in range(1, 16)]
address_columns_to_check.extend(license_columns)

identifier_columns = [f'{NPPES_PREFIX}Other Provider Identifier State_{i}' for i in range(1, 51)]
address_columns_to_check.extend(identifier_columns)

provider_license_number_columns = [f'{NPPES_PREFIX}Provider License Number_{i}' for i in range(1, 16)]
taxonomy_columns_to_check = [f'{NPPES_PREFIX}Healthcare Provider Taxonomy Code_{i}' for i in range(1, 16)]

NPPES_FIRST_NAME_COL_NAME = add_source_db_prefix('Provider First Name', NPPES_PREFIX)
NPPES_MIDDLE_NAME_COL_NAME = add_source_db_prefix('Provider Middle Name', NPPES_PREFIX) 
NPPES_LAST_NAME_COL_NAME = add_source_db_prefix('Provider Last Name (Legal Name)', NPPES_PREFIX) 
NPPES_ENTITY_TYPE_CODE = add_source_db_prefix('Entity Type Code', NPPES_PREFIX)
NPPES_NPI_COL_NAME = add_source_db_prefix('NPI', NPPES_PREFIX)
NPPES_ENTITY_TYPE_ORG_CODE = 2

NPPES_IN_RI_COL_NAME = add_source_db_prefix('Is In RI?', NPPES_PREFIX + 'CALC_')
NPPES_MATCH_RIDOH_NAME_COL_NAME = add_source_db_prefix('Matched RIDOH On Name', NPPES_PREFIX + 'CALC_')
NPPES_MATCH_RIDOH_LIC_COL_NAME = add_source_db_prefix('Matched RIDOH On License', NPPES_PREFIX + 'CALC_')

chunk_size = 200000 
base_path = '.'
nppes_file_name = os.path.join(INPUT_FILES_DIRECTORY, nppes_provider_data_file)
output_ri_providers_file_name = current_date() + output_ri_nppes_clinicians_file_suffix
output_ri_providers_file_path = os.path.join(base_path, output_ri_providers_file_name)

# %%
nppes_total_rows = sum(1 for _ in open(nppes_file_name))
num_chunks = nppes_total_rows // chunk_size + (nppes_total_rows % chunk_size > 0)
print(f"The file will be read in {num_chunks} chunks. The target number of NPI numbers to find is: ", len(unique_APCD_npis))

nppes_aggregated = pd.DataFrame()
for chunk_index, current_npi_batch in enumerate(import_csv_gracefully(INPUT_FILES_DIRECTORY, nppes_provider_data_file, chunk_size, False)):
	current_npi_batch = current_npi_batch.add_prefix(NPPES_PREFIX)
	# Filter on only those NPI numbers that were returned from the APCD query - all other NPI numbers will be disregarded!  
	current_npi_batch = current_npi_batch[current_npi_batch[NPPES_NPI_COL_NAME].isin(unique_APCD_npis)]

	print("Current Time:", current_date_time(), " Chunk number: ", chunk_index + 1, " Filtered Chunk length:", len(current_npi_batch))
	if exclude_organizations:
		current_npi_batch = current_npi_batch[current_npi_batch[NPPES_ENTITY_TYPE_CODE] != NPPES_ENTITY_TYPE_ORG_CODE]
	
	# We nevertheless do additional filtering to actually make sure that the providers we got back from NPPES are actually 
	# Rhode Island Providers! (hence filtering on state, RIDOH name, and RIDOH license number - looking for proof of residency!)
	in_ri = current_npi_batch[current_npi_batch[address_columns_to_check].apply(lambda x: (x == RHODE_ISLAND_STATE_CODE).any(), axis=1)].copy()
	in_ri[NPPES_IN_RI_COL_NAME] = True
	in_ri.dropna(how='all', axis=1, inplace=True)

	nppes_aggregated = pd.concat([nppes_aggregated, in_ri], ignore_index=True)

	not_in_ri = current_npi_batch[~current_npi_batch[address_columns_to_check].apply(lambda x: (x == RHODE_ISLAND_STATE_CODE).any(), axis=1)].copy()
	not_in_ri[NPPES_IN_RI_COL_NAME] = False
	print("RI analysis complete. There were the following number of providers not in RI:" , len(not_in_ri), " ", current_date_time())
	
	if (len(not_in_ri) > 0):
		final_mask_names = (
			not_in_ri[NPPES_FIRST_NAME_COL_NAME].isin(ridoh_first_names) & 
			not_in_ri[NPPES_LAST_NAME_COL_NAME].isin(ridoh_last_names)
		)

		filtered_name_rows = not_in_ri.loc[final_mask_names].copy()
		filtered_name_rows[NPPES_MATCH_RIDOH_NAME_COL_NAME] = True
		print("Name matching analysis complete.", len(filtered_name_rows[filtered_name_rows[NPPES_MATCH_RIDOH_NAME_COL_NAME] == True]), " providers matched on name. ", current_date_time())

		final_mask_licenses = pd.DataFrame({col: not_in_ri[col].isin(ridoh_licenses) for col in provider_license_number_columns}).any(axis=1)
		filtered_license_rows = not_in_ri.loc[final_mask_licenses].copy()
		filtered_license_rows[NPPES_MATCH_RIDOH_LIC_COL_NAME] = True
		print("License matching analysis complete. ", len(filtered_license_rows[filtered_license_rows[NPPES_MATCH_RIDOH_LIC_COL_NAME] == True]), " providers matched on license, ", current_date_time())

		filtered_rows = pd.concat([filtered_name_rows,filtered_license_rows])
		filtered_rows.dropna(how='all', axis=1, inplace=True)

		nppes_aggregated = pd.concat([nppes_aggregated, filtered_rows], ignore_index=True)
	print("****** Total providers are now: ", len(nppes_aggregated))
	# Improved memory usage on local machine but may be unnecessary
	for var in ['current_npi_batch', 'in_ri', 'not_in_ri', 'filtered_name_rows', 'filtered_license_rows', 'filtered_rows', 'final_mask_names', 'final_mask_licenses']:
		if var in locals():
			del locals()[var]


nppes_aggregated.to_csv(output_ri_providers_file_path, index=False)

# %% [markdown]
# ### Merge APCD to NPPES
# - This is where the logic of actually creating one single large dataset takes place
# - Of note, we consider all NPIs who EITHER billed for our core prevention codes OR who billed only for vaccination but did not have an internal medicine subspecialty of exclusion 
# - We also save down those APCD NPIs that were not found in NPPES for further evaluation and analysis

# %%
nppes_data = import_csv_gracefully('.', output_ri_providers_file_name)
merged_df = pd.merge(apcd_provider_data, nppes_data, left_on=APCD_NPI_COL_NAME, right_on=NPPES_NPI_COL_NAME)
print("After merging with NPPEs, only", len(merged_df) , "NPIs remain.")


npis_not_in_nppes = apcd_provider_data[~apcd_provider_data[APCD_NPI_COL_NAME].isin(nppes_data[NPPES_NPI_COL_NAME])]
print("This means that", len(npis_not_in_nppes), "NPIs from APCD are not found in NPPES. (likely because they are not RI providers)")
print("Of these,", len(npis_not_in_nppes[npis_not_in_nppes[APCD_TOTAL_CLAIMS_ALL_COL_NAME] <= 100]), "billed for 100 or fewer claims of ANY kind.")
npis_not_in_nppes.to_csv("npis_not_in_nppes_" + current_date() + ".csv")


providers_with_core_prevention = merged_df[merged_df[APCD_PC_CODES_PRESENT_COL_NAME] == True]
print("The", len(merged_df),"NPIs included", len(providers_with_core_prevention), "providers who did bill for one of our core prevention codes at least once.")
providers_without_core_prevention = merged_df[merged_df[APCD_PC_CODES_PRESENT_COL_NAME] == False]
print("And it included", len(providers_without_core_prevention), "providers who did NOT bill for one of our core prevention codes at least once.")

df_nppes_all_taxonomies = import_csv_gracefully(INPUT_FILES_DIRECTORY, 'nppes_all_taxonomies.csv')
internal_medicine_subspecialties_to_exclude = df_nppes_all_taxonomies[df_nppes_all_taxonomies['Internal_Medicine_Subspecialty_To_Exclude'] == 'Yes'][NPPES_CODE].tolist()

valid_columns = [col for col in taxonomy_columns_to_check if col in providers_without_core_prevention.columns]

if valid_columns:
    mask = providers_without_core_prevention[valid_columns].isin(internal_medicine_subspecialties_to_exclude).any(axis=1)
else:
    mask = pd.Series([False] * len(providers_without_core_prevention))

providers_without_core_prevention_specialty = providers_without_core_prevention[mask]
print("Of the providers who billed only for immunizations,",len(providers_without_core_prevention_specialty), "providers had a specialty of exclusion.")
providers_without_core_prevention_no_specialty = providers_without_core_prevention[~mask]
print("Of the providers who billed only for immunizations,",len(providers_without_core_prevention_no_specialty), "providers did not have a specialty of exclusion.")

final_provider_list = pd.concat([providers_with_core_prevention, providers_without_core_prevention_no_specialty], ignore_index=True)
print("In total then, there were: ", len(final_provider_list), "primary care NPIs that could be matched between NPPES, RIDOH and APCD.")

# %% [markdown]
# # Merged Dataset Cleaning
# 
# ### Taxonomy Mark-up
# - This section of code deals with using the taxonomy codes included in NPPES to identify what types of clinicians the merged file includes

# %%
physician_taxonomies = df_nppes_all_taxonomies[df_nppes_all_taxonomies['Grouping'] == 'Allopathic & Osteopathic Physicians']
# We are excluding those taxonomies that we do not expect are primary care taxonomies
df_nppes_pc_taxonomies = df_nppes_all_taxonomies[df_nppes_all_taxonomies['Exclude?'] != 'Yes']

# Create dictionary mapping of physician specialty to taxonomy codes
taxoncmy_code_specialties = {}
# Though EM should not generally be primary care, due to some data quality issues, we include it here
taxoncmy_code_specialties[SPECIALTY_EM] = physician_taxonomies[(physician_taxonomies[NPPES_CLASSIFICATION] == SPECIALTY_EM)][NPPES_CODE].tolist()
# anyone with one of these specialties must be a physician by defintion
primary_care_adjacent_specialties = [SPECIALTY_INTEG_MEDICINE, SPECIALTY_PREVENT_MEDICINE, SPECIALTY_INTERNAL_MEDICINE, SPECIALTY_FAMILY_MEDICINE, SPECIALTY_GEN_PRACTICE, SPECIALTY_OBGYN, SPECIALTY_PEDS]
for specialty in primary_care_adjacent_specialties:
    taxoncmy_code_specialties[specialty] = df_nppes_pc_taxonomies[(df_nppes_pc_taxonomies[NPPES_CLASSIFICATION] == specialty)][NPPES_CODE].tolist()

def get_codes(dataframe, classification_conditions=None):
    """Get taxonomy codes based on classification conditions.
    Args:
        dataframe: DataFrame containing taxonomy codes
        classification_conditions: Single classification string or list of tuples with (classification, operator)
            where operator is '==' or '|' for OR condition
    """
    if isinstance(classification_conditions, str):
        return dataframe[dataframe[NPPES_CLASSIFICATION] == classification_conditions][NPPES_CODE].tolist()
    elif classification_conditions:
        mask = None
        for classification in classification_conditions:
            condition = (dataframe[NPPES_CLASSIFICATION] == classification)
            if mask is None:
                mask = condition
            else:
                mask |= condition  # Bitwise OR for OR condition
        return dataframe[mask][NPPES_CODE].tolist()
    return dataframe[NPPES_CODE].tolist()

valid_columns = [col for col in taxonomy_columns_to_check if col in final_provider_list.columns]

# Of note, the logic below will take the last role as the accurate one - this is why we set up the boolean
# is_role column to track those clinicians who meet the criteria for more than one role (this logic is also replicated for specialty) 
taxonomy_code_roles = {
    ROLE_MISC_OTHER : get_codes(df_nppes_all_taxonomies,['Legal Medicine', 'Specialist']),
    ROLE_PODIATRY : get_codes(df_nppes_all_taxonomies, ROLE_PODIATRY),
    ROLE_OPTOMETRY : get_codes(df_nppes_all_taxonomies, ROLE_OPTOMETRY),
    ROLE_CASE_MGMT : get_codes(df_nppes_all_taxonomies, ROLE_CASE_MGMT),
    ROLE_PSYCHOLOGIST : get_codes(df_nppes_all_taxonomies, ROLE_PSYCHOLOGIST),
    ROLE_ORGANIZATION : get_codes(df_nppes_pc_taxonomies, ['Clinic/Center','General Acute Care Hospital', 'Nursing Facility/Intermediate Care Facility', 'Hospice Care, Community Based']),
    ROLE_CLIN_NURSE_SPECIALIST : get_codes(df_nppes_pc_taxonomies, ROLE_CLIN_NURSE_SPECIALIST),
    ROLE_CERT_NURSE_MIDWIFE : get_codes(df_nppes_pc_taxonomies, ['Advanced Practice Midwife', 'Midwife']),
    ROLE_NURSE: get_codes(df_nppes_pc_taxonomies, 'Registered Nurse'), 
    ROLE_STUDENT : get_codes(df_nppes_pc_taxonomies, 'Student in an Organized Health Care Education/Training Program'), 
    ROLE_NP: get_codes(df_nppes_pc_taxonomies, ROLE_NP),
    ROLE_PA: get_codes(df_nppes_pc_taxonomies, ROLE_PA),
    ROLE_MD_DO: get_codes(physician_taxonomies),
}

def update_roles_specialties(final_provider_list, taxonomy_dictionary, valid_columns, col_to_update):
    for role_or_specialty, codes_to_check in taxonomy_dictionary.items():
        if valid_columns:
            mask = final_provider_list[valid_columns].apply(lambda row: row.isin(codes_to_check).any(), axis=1)
            final_provider_list.loc[mask, col_to_update] = role_or_specialty
            taxonomy_code_tracking_col_name = ''
            if col_to_update == SPECIALTY_COLUMN_NAME:
                taxonomy_code_tracking_col_name = 'Included_taxonomy_values_specialty'
            elif col_to_update == RIDOH_CREDENTIAL_COLUMN_NAME:
                taxonomy_code_tracking_col_name = 'Included_taxonomy_values_role'
            final_provider_list.loc[mask, taxonomy_code_tracking_col_name] = final_provider_list.loc[mask, valid_columns].apply(
                lambda row: [val for val in row if val in codes_to_check], axis=1
            )
            column_name = f'is_{role_or_specialty}'
            final_provider_list[column_name] = False  
            final_provider_list.loc[mask, column_name] = True 

update_roles_specialties(final_provider_list, taxonomy_code_roles, valid_columns, RIDOH_CREDENTIAL_COLUMN_NAME)
update_roles_specialties(final_provider_list, taxoncmy_code_specialties, valid_columns, SPECIALTY_COLUMN_NAME)


# Original list of column names
full_specialty_list = primary_care_adjacent_specialties + [SPECIALTY_EM]
cols = [f'is_{col}' for col in full_specialty_list]

# Create the 'Count Specialties' column by summing up True values in the modified columns
final_provider_list['Count Specialties'] = final_provider_list[cols].sum(axis=1)
final_provider_list['Derived Specialty'] = 'Unknown'

# Function to determine the derived specialty based on Count Specialties and specific rules
def get_derived_specialty(row):
    if row['Count Specialties'] == 1:
        # Find the column where the value is True and remove the 'is_' prefix
        true_column = next(col for col in cols if row[col] == True)
        return true_column.replace('is_', '')
    elif row['Count Specialties'] == 2:
        # Get the list of columns where value is True
        true_columns = [col for col in cols if row[col] == True]
        if set(true_columns) == {'is_Internal Medicine', 'is_Pediatrics'}:
            return 'Med-Peds'
        elif set(true_columns) == {'is_Internal Medicine', 'is_Family Medicine'}:
            return 'IM-FM'
        elif set(true_columns) == {'is_Emergency Medicine', 'is_Internal Medicine'}:
            return 'EM-IM'
        elif set(true_columns) == {'is_Family Medicine', 'is_Obstetrics & Gynecology'}:
            return 'FM-OBGYN'
        elif set(true_columns) == {'is_Family Medicine', 'is_Pediatrics'}:
            return 'FM-Peds'
        elif set(true_columns) == {'is_Family Medicine', 'is_Emergency Medicine'}:
            return 'FM-EM'
        else:
            return 'Multiple specialties'
    elif row['Count Specialties'] > 2:
        return 'Multiple specialties'
    else:
        return 'Unknown'

final_provider_list['Derived Specialty'] = final_provider_list.apply(get_derived_specialty, axis=1)

final_provider_list.to_csv('final_provider_list.csv')

# %% [markdown]
# ### RIDOH License Number Triangulation
# The goal of this code is to try to triangulate the exact license number and specialty type of every clinician found above. 
# The license number is particularly valuable as it can be directly looked up for information on year of graduation as well as school

# %%
import re

def clean_license_minimal(license_no):
    if pd.isna(license_no) or license_no.strip() == '':
        return ''
    pattern = rf'^({MD_PREFIX}|{DO_PREFIX}|{LP_PREFIX})'
    return re.sub(pattern, '', license_no).strip()

def clean_license(license_no):
    if pd.isna(license_no) or license_no.strip() == '':
        return ''
    pattern = rf'^({MD_PREFIX}0?|{DO_PREFIX}0?|{LP_PREFIX}0?)'
    return re.sub(pattern, '', license_no).strip()

ridoh_clinicians[SPECIALTY_COLUMN_NAME] = ridoh_clinicians[SPECIALTY_COLUMN_NAME].astype(str).replace(NAN_STRING, '')

grouped_ridoh = ridoh_clinicians.groupby(
        [RIDOH_CREDENTIAL_COLUMN_NAME, RIDOH_NAME_COL_NAME, RIDOH_FIRST_NAME_COL_NAME, 
         RIDOH_MIDDLE_COL_NAME, RIDOH_LAST_NAME_COL_NAME, RIDOH_LICENSE_NO_COL_NAME, 
         RIDOH_LIC_TYPE_COL_NAME, RIDOH_STATUS_COL_NAME, RIDOH_ISSUE_DATE_COL_NAME, RIDOH_EXP_DATE_COL_NAME, 
         RIDOH_ADDRESS_ONE_COL_NAME, RIDOH_ADDRESS_TWO_COL_NAME, RIDOH_ADDRESS_THREE_COL_NAME, 
         RIDOH_CITY_COL_NAME, RIDOH_STATE_COL_NAME, RIDOH_ZIP_COL_NAME, 
         RIDOH_EMAIL_COL_NAME,RIDOH_PHONE_COL_NAME, RIDOH_FAX_COL_NAME, RIDOH_PROF_COL_NAME], dropna=False
    )[SPECIALTY_COLUMN_NAME].apply(','.join).reset_index()

grouped_ridoh['License Cleaned'] = grouped_ridoh[RIDOH_LICENSE_NO_COL_NAME].apply(clean_license)
grouped_ridoh['License Cleaned Minimal'] = grouped_ridoh[RIDOH_LICENSE_NO_COL_NAME].apply(clean_license_minimal)


grouped_ridoh['full_name_concatenated'] = (
    grouped_ridoh[RIDOH_FIRST_NAME_COL_NAME].fillna('') + ' ' +
    grouped_ridoh[RIDOH_MIDDLE_COL_NAME].fillna('') + ' ' +
    grouped_ridoh[RIDOH_LAST_NAME_COL_NAME].fillna('')
).str.strip().str.strip().str.lower().str.replace(' ', '')

def clean_provider_licenses(row):
    licenses = [
        clean_license(row[add_source_db_prefix('Provider License Number_1', NPPES_PREFIX)]), 
        clean_license(row[add_source_db_prefix('Provider License Number_2', NPPES_PREFIX)]),
        clean_license(row[add_source_db_prefix('Provider License Number_3', NPPES_PREFIX)]),
        clean_license(row[add_source_db_prefix('Provider License Number_4', NPPES_PREFIX)]),
        clean_license(row[add_source_db_prefix('Provider License Number_5', NPPES_PREFIX)])
    ]
    return ', '.join(filter(None, licenses))

final_provider_list['License Cleaned'] = final_provider_list.apply(clean_provider_licenses, axis=1)

def confirm_license_specialty(row, ridoh_clinicians):
    matching_row = ridoh_clinicians[
        (ridoh_clinicians[RIDOH_FIRST_NAME_COL_NAME].str.strip().str.lower() == str(row[NPPES_FIRST_NAME_COL_NAME]).strip().lower()) &
        (ridoh_clinicians[RIDOH_LAST_NAME_COL_NAME].str.strip().str.lower() == str(row[NPPES_LAST_NAME_COL_NAME]).strip().lower()) &
        (ridoh_clinicians[RIDOH_CREDENTIAL_COLUMN_NAME].str.strip().str.lower() == str(row[RIDOH_CREDENTIAL_COLUMN_NAME]).strip().lower())
    ]
    
    # Usually first/last name is uniquely identify - however, in some cases, we need to 
    if (len(matching_row)) > 1:
        if pd.notna(row[NPPES_MIDDLE_NAME_COL_NAME]) and row[NPPES_MIDDLE_NAME_COL_NAME].strip() != '':
            matching_row = matching_row[matching_row[RIDOH_MIDDLE_COL_NAME].str.strip().str.lower().str[0] == str(row[NPPES_MIDDLE_NAME_COL_NAME]).strip().lower()[0]]
    
    if (len(matching_row) == 0):
        row_full_name = (
                str(row[NPPES_FIRST_NAME_COL_NAME] if pd.notna(row[NPPES_FIRST_NAME_COL_NAME]) else '').strip().lower() +
                str(row[NPPES_MIDDLE_NAME_COL_NAME] if pd.notna(row[NPPES_MIDDLE_NAME_COL_NAME]) else '').strip().lower() +
                str(row[NPPES_LAST_NAME_COL_NAME] if pd.notna(row[NPPES_LAST_NAME_COL_NAME]) else '').strip().lower()
            )
        matching_row = ridoh_clinicians[
            (ridoh_clinicians['full_name_concatenated'].str.strip().str.lower().str.replace(' ', '') == row_full_name.replace(' ', '')) &
            (ridoh_clinicians[RIDOH_CREDENTIAL_COLUMN_NAME].str.strip().str.lower() == str(row[RIDOH_CREDENTIAL_COLUMN_NAME]).strip().lower())
        ]

    if not matching_row.empty:
        for license_column in ['License Cleaned', 'License Cleaned Minimal']:
            ridoh_license_cleaned = matching_row[license_column].values[0]
            final_provider_license_cleaned = row['License Cleaned']

            if ridoh_license_cleaned in [item.strip() for item in final_provider_license_cleaned.split(',')]:
                return pd.Series({
                    'Confirmed License': matching_row[RIDOH_LICENSE_NO_COL_NAME].values[0],
                    'Confirmed RIDOH Specialty': matching_row[SPECIALTY_COLUMN_NAME].values[0]
                })
        
    return pd.Series({
        'Confirmed License': UNCONFIRMED_STRING,
        'Confirmed RIDOH Specialty': UNCONFIRMED_STRING
    })

final_provider_list[['Confirmed License', 'Confirmed RIDOH Specialty']] = final_provider_list.apply(
    confirm_license_specialty, axis=1, ridoh_clinicians=grouped_ridoh
)

final_provider_list.to_csv('final_provider_list.csv')

# %%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

MAX_WAIT_TIME_IN_SECS = 3

METHOD_LIC = 'License Look-up'
METHOD_NAME = 'First and Last Name Look-up'

HLTHRI_ISSUE_DATE_COL_NAME = 'RIDOH Issue Date'
HLTHRI_EXP_DATE_COL_NAME = 'RIDOH Expiration Date'
HLTHRI_SCHOOL_NAME_COL_NAME = 'RIDOH School Name'
HLTHRI_GRAD_DATE_COL_NAME = 'RIDOH Graduation Date'
HLTHRI_SPEC_INFO_COL_NAME = 'RIDOH Specialty Info'
HLTHRI_METHD_COL_NAME = 'RIDOH Methodology'
HLTHRI_LIC_NO_COL_NAME = 'RIDOH Discovered License No'
HLTHRI_NAME_COL_NAME = 'RIDOH Discovered Name'
HLTHRI_PROF_COL_NAME = 'RIDOH Discovered Profession'
HLTHRI_LIC_TYPE_COL_NAME = 'RIDOH Discovered License Type'
HLTHRI_LIC_STATUS_COL_NAME = 'RIDOH Discovered License Status'
HLTHRI_CITY_COL_NAME = 'RIDOH Discovered City'
HLTHRI_STATE_COL_NAME = 'RIDOH Discovered State'

MAX_RETRIES_PER_CLINICIAN = 2

final_provider_list_2 = final_provider_list.copy()
final_provider_list_2[HLTHRI_ISSUE_DATE_COL_NAME] = ''
final_provider_list_2[HLTHRI_EXP_DATE_COL_NAME] = ''
final_provider_list_2[HLTHRI_SCHOOL_NAME_COL_NAME] = ''
final_provider_list_2[HLTHRI_GRAD_DATE_COL_NAME] = ''
final_provider_list_2[HLTHRI_SPEC_INFO_COL_NAME] = ''
final_provider_list_2[HLTHRI_METHD_COL_NAME] = ''
final_provider_list_2[HLTHRI_LIC_NO_COL_NAME] = ''
final_provider_list_2[HLTHRI_NAME_COL_NAME] = ''
final_provider_list_2[HLTHRI_PROF_COL_NAME] = ''
final_provider_list_2[HLTHRI_LIC_TYPE_COL_NAME] = ''
final_provider_list_2[HLTHRI_LIC_STATUS_COL_NAME] = ''
final_provider_list_2[HLTHRI_CITY_COL_NAME] = ''
final_provider_list_2[HLTHRI_STATE_COL_NAME] = ''

ridoh_online_verification_complaint_submission_site = 'https://healthri.mylicense.com/verification/Search.aspx?facility=N&SubmitComplaint=Y'

def get_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--headless")  # Enable headless mode
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model (Linux only)
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    chrome_options.add_argument("--log-level=3")  # Suppress logs
    driver = webdriver.Chrome(options=chrome_options)
    return driver




def get_element_text(wait, element_name, primary_id, secondary_id=None, verbose=False):
    try:
        return wait.until(EC.presence_of_element_located((By.ID, primary_id))).text
    except TimeoutException:
        try:
            if secondary_id is not None:
                return wait.until(EC.presence_of_element_located((By.ID, secondary_id))).text
            else:
                return None
        except TimeoutException:
            if verbose:
                print(f"{element_name} element not found.")
            return None

def get_nppes_license_if_available(row):
    license_to_search = None
    if (row['Confirmed License'] != UNCONFIRMED_STRING):
        license_to_search = row['Confirmed License']
    else:
        for i in range(1, 6):
            license_number_col = f'{NPPES_PREFIX}Provider License Number_{i}'
            state_code_col = f'{NPPES_PREFIX}Provider License Number State Code_{i}'
            if row[state_code_col] == RHODE_ISLAND_STATE_CODE:
                license_to_search = row[license_number_col]
                # Thought process / hope is first license is more likely to be accurate
                # Could revisit searching against all possible licenses but currently will just fall back
                # to name search - it also turns out licenses CAN have dashes and be valid but CAN'T have spaces!
                return license_to_search.replace(" ","")

def clean_name(name):
    return name.replace("-"," ").replace(" ", "")



for index, row in final_provider_list_2.iterrows():
    role = row[RIDOH_CREDENTIAL_COLUMN_NAME]
    first_name = row[NPPES_FIRST_NAME_COL_NAME]
    last_name = row[NPPES_LAST_NAME_COL_NAME]
    entity_type = row[NPPES_ENTITY_TYPE_CODE]
    if entity_type == NPPES_ENTITY_TYPE_ORG_CODE:
        print(f"Index: {index} - No license look-up for organizations")
        continue

    # If there is no first or last name, then there is a good chance this is an organization and there is no need to search
    if not first_name or not last_name or str(first_name).lower() == NAN_STRING or str(last_name).lower() == NAN_STRING:
        print(f"Index: {index} - Skipping no name clinician.")
        continue
    skip_license_search = False
    for attempt in range(MAX_RETRIES_PER_CLINICIAN):
        # resetting those variables that can change per attempt
        license_to_search = None
        credential = None
        prefix = None 
        try:
            # Currently only attempting license look-ups for specific credentials 
            if (role in [ROLE_MD_DO, ROLE_PA, ROLE_CLIN_NURSE_SPECIALIST, ROLE_NURSE, ROLE_NP, ROLE_CERT_NURSE_MIDWIFE]):
                driver = get_chrome_driver()
                driver.get(ridoh_online_verification_complaint_submission_site)

                # the skip_license_search flag was developed in the case that the result of the search based on the license number
                # yields a physician whose name doesn't match our records - in this case, we want to re-try to search based on name alone
                if not skip_license_search:
                    license_to_search = get_nppes_license_if_available(row)

                
                # Of note, RI is not considered a valid prefix by the RIDOH website - however, there are no 
                # guarantees on if this belongs to an MD, DO, NP so for now, falling back on name searches
                if license_to_search is not None:
                    invalid_license_pattern = rf'^({RHODE_ISLAND_STATE_CODE})'
                    invalid_match = re.match(invalid_license_pattern, license_to_search)
                    if invalid_match:
                        skip_license_search = True
                        raise Exception(f"License begins with {RHODE_ISLAND_STATE_CODE} identifer - will search on name instead!")

                    # Consider better handling for: F03170623, LP03291, MW00016
                    pattern = rf'^({DO_PREFIX}|{MD_PREFIX}|{CNM_PREFIX}|{PA_PREFIX}|{APRN_PREFIX}|{RN_PREFIX}|{ETL_PREFIX}|{NPP_PREFIX}|{CAPRN_PREFIX})'
                    match = re.match(pattern, license_to_search)

                    if match:
                        prefix = match.group(0)

                    if prefix == PA_PREFIX:
                        credential = ROLE_PA
                    elif prefix == MD_PREFIX or prefix == DO_PREFIX:
                        credential = ROLE_MD_DO
                    elif prefix in [APRN_PREFIX,RN_PREFIX,ETL_PREFIX,NPP_PREFIX,CAPRN_PREFIX]:
                        credential = 'Nursing'
                    elif prefix == CNM_PREFIX:
                        credential = 'Midwifery'

                    print(f"Index: {index} - searching {license_to_search}, against the license type of: {credential},{current_date_time()}")

                    if credential is not None:
                        dropdown_element = driver.find_element(By.ID, 't_web_lookup__profession_name')
                        select = Select(dropdown_element)
                        select.select_by_visible_text(credential)

                    search_input = driver.find_element(By.NAME, "t_web_lookup__license_no")
                    search_input.send_keys(license_to_search)  
                    methodology = METHOD_LIC
                else:
                    if role == ROLE_MD_DO:
                        credential = ROLE_MD_DO
                    elif role == ROLE_PA:
                        credential = ROLE_PA
                    elif role == ROLE_NP or role == ROLE_NURSE or role == ROLE_CLIN_NURSE_SPECIALIST:
                        credential = 'Nursing'
                    elif role == ROLE_CERT_NURSE_MIDWIFE:
                        credential = 'Midwifery'
                    else:
                        break

                    dropdown_element = driver.find_element(By.ID, 't_web_lookup__profession_name')
                    select = Select(dropdown_element)
                    select.select_by_visible_text(credential)

                    search_input = driver.find_element(By.NAME, "t_web_lookup__first_name")
                    search_input.send_keys(first_name)  

                    search_input = driver.find_element(By.NAME, "t_web_lookup__last_name")
                    search_input.send_keys(last_name) 
                    print(f"Index: {index} - searching {first_name} and {last_name}, against the license type of: {credential}, {current_date_time()}")
                    methodology = METHOD_NAME
                
                search_input.send_keys(Keys.RETURN)
                wait = WebDriverWait(driver, MAX_WAIT_TIME_IN_SECS)

                wait.until(EC.presence_of_element_located((By.ID, 'datagrid_results')))
                link_elements = driver.find_elements(By.CSS_SELECTOR, 'a[id^="datagrid_results__ctl"]')

                for link in link_elements:
                    license_number = None
                    try:
                        link_license_number = link.find_element(By.XPATH, '../following-sibling::td[1]/span').text
                        # This logic is necessary as the search automatically inserts a wildcard at beginning and thus
                        # includes other associated licenses which capture slightly different info (e.g. lack specialty for physicians)
                        # here, we confirm that we got the exact license we searched on if license was in the query
                        if license_to_search is not None and license_to_search != link_license_number:
                            continue
                        
                        ridoh_name = link.text
                        print(f'Name: {ridoh_name} and License Number: {link_license_number} found based on methodology: {methodology}')

                        # This logic is necessary because there are individuals who an incorrect license number listed and whose
                        # name on license look-up doesn't match - for these people, we want to revert to a manual name search
                        if (clean_name(first_name) not in clean_name(ridoh_name) or clean_name(last_name) not in clean_name(ridoh_name)):
                            skip_license_search = True
                            raise Exception("Name mismatch based on license search!")
                        
                        ridoh_profession = link.find_element(By.XPATH, '../following-sibling::td[3]/span').text
                        ridoh_license_type = link.find_element(By.XPATH, '../following-sibling::td[4]/span').text
                        ridoh_license_status = link.find_element(By.XPATH, '../following-sibling::td[5]/span').text
                        ridoh_city = link.find_element(By.XPATH, '../following-sibling::td[6]/span').text
                        ridoh_state = link.find_element(By.XPATH, '../following-sibling::td[7]/span').text

                        final_provider_list_2.at[index, HLTHRI_NAME_COL_NAME] = ridoh_name
                        final_provider_list_2.at[index, HLTHRI_LIC_NO_COL_NAME] = link_license_number
                        final_provider_list_2.at[index, HLTHRI_PROF_COL_NAME] = ridoh_profession
                        final_provider_list_2.at[index, HLTHRI_LIC_TYPE_COL_NAME] = ridoh_license_type
                        final_provider_list_2.at[index, HLTHRI_LIC_STATUS_COL_NAME] = ridoh_license_status
                        final_provider_list_2.at[index, HLTHRI_CITY_COL_NAME] = ridoh_city
                        final_provider_list_2.at[index, HLTHRI_STATE_COL_NAME] = ridoh_state

                        driver.execute_script("arguments[0].removeAttribute('target');", link)
                        link.click()
                        break
                    except StaleElementReferenceException:
                        print("Stale element reference, re-fetching the links.")

                issue_date = get_element_text(wait, "Issue date", "_ctl15__ctl1_issue_date", "_ctl17__ctl1_issue_date")
                expiration_date = get_element_text(wait, "Expiration date", "_ctl15__ctl1_expiration_date", "_ctl17__ctl1_expiration_date")
                school_name = get_element_text(wait, "School name", "_ctl25__ctl1_schl_name", "_ctl27__ctl1_schl_name")
                graduated_date = get_element_text(wait, "Graduation date", "_ctl25__ctl1_date_to", "_ctl27__ctl1_date_to")
                specialty_info = get_element_text(wait, "Specialty Information", "_ctl33__ctl1_authority_code")

                if all(x is None for x in [issue_date, expiration_date, school_name, graduated_date, specialty_info]) and methodology == METHOD_LIC:
                    skip_license_search = True
                    raise Exception("Nothing was found based on a license search - retrying based on name")


                final_provider_list_2.at[index, HLTHRI_ISSUE_DATE_COL_NAME] = issue_date
                final_provider_list_2.at[index, HLTHRI_EXP_DATE_COL_NAME] = expiration_date
                # Of note, school name is particularly complicated for non-physicians - it seems to capture more school info 
                final_provider_list_2.at[index, HLTHRI_SCHOOL_NAME_COL_NAME] = school_name
                final_provider_list_2.at[index, HLTHRI_GRAD_DATE_COL_NAME] = graduated_date
                # Of note, physicians can in fact have multiple specialties list - current logic doesn't handle this
                final_provider_list_2.at[index, HLTHRI_SPEC_INFO_COL_NAME] = specialty_info
                final_provider_list_2.at[index, HLTHRI_METHD_COL_NAME] = methodology
                driver.quit()
                break
        except Exception as e:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES_PER_CLINICIAN} - Error processing row {index}: {e}")
            if attempt == MAX_RETRIES_PER_CLINICIAN - 1:
                print("Max retries reached. Skipping to the next row.")
                driver.quit()
            else:
                continue

final_provider_list_2.to_csv('final_modified_dataframe.csv', index=False)

# %% [markdown]
# # Geographic Analysis and Breakdown

# %%
final_provider_list_3 = import_csv_gracefully('.', 'final_modified_dataframe.csv')
school_data = import_csv_gracefully(INPUT_FILES_DIRECTORY, 'educational_institutional_lookup.csv')

combined = pd.merge(final_provider_list_3, school_data, left_on= HLTHRI_SCHOOL_NAME_COL_NAME, right_on='Institution', how='left')
# combined.to_csv('final_modified_dataframe_with_state.csv', index=False)
import plotly.graph_objects as go
import ipywidgets as widgets
import plotly.express as px
import plotly.graph_objects as go

ALL_VALUE = 'ALL'

decade_color_mapping = {
    '2000.0': '#1f77b4',  # Blue
    '2010.0': '#ff7f0e',  # Orange
    '2020.0': '#2ca02c',  # Green
    '1990.0': '#d62728',  # Red
    '1980.0': '#9467bd',  # Purple
    '1970.0': '#8c564b',  # Brown
    '1960.0': '#e377c2',  # Pink
    '1950.0': '#7f7f7f',  # Gray
    '1940.0': '#bcbd22',  # Olive
    '1930.0': '#17becf',  # Cyan
    'OTHER' : '#f0eded'
}

state_color_mapping = {
    'OTHER' : '#f0eded',
    'Massachusetts': '#1f77b4',  # Blue
    'Rhode Island': '#ff7f0e',  # Orange
    'New York': '#2ca02c',  # Green
    'Pennsylvania': '#d62728',  # Red
    'Maine': '#9467bd',  # Purple
    'Connecticut': '#8c564b',  # Brown
    'North Carolina': '#e377c2',  # Pink
    'California': '#7f7f7f',  # Gray
    'Missouri': '#bcbd22',  # Olive
    'Texas': '#17becf',  # Cyan
    'Ohio': '#ffbb78',  # Light Orange
    'District of Columbia': '#98df8a',  # Light Green
    'New Jersey': '#ff9896',  # Light Red
    'Kentucky': '#c5b0d5',  # Lavender
    'Colorado': '#f7b6d2',  # Light Pink
    'Florida': '#c49c94',  # Light Brown
    'Georgia': '#f4c542',  # Light Olive
    'Illinois': '#17b3c2',  # Teal
    'Maryland': '#7f7f7f',  # Dark Gray
    'Michigan': '#bd9d29',  # Mustard Yellow

}

def get_colors(items, color_mapping):
    return [color_mapping.get(item, '#000000') for item in items]  # Default to black if no mapping exists


combined.fillna('Unknown', inplace=True)
combined[HLTHRI_GRAD_DATE_COL_NAME] = pd.to_datetime(combined[HLTHRI_GRAD_DATE_COL_NAME], errors='coerce')
combined['Graduation_Decade'] = (combined[HLTHRI_GRAD_DATE_COL_NAME].dt.year // 10 * 10).astype(str)

excluded_credentials = [ROLE_STUDENT, ROLE_ORGANIZATION, ROLE_MISC_OTHER, ROLE_CASE_MGMT, ROLE_PSYCHOLOGIST, ROLE_PODIATRY, ROLE_OPTOMETRY]
filtered_credentials = [cred for cred in combined[RIDOH_CREDENTIAL_COLUMN_NAME].unique() if cred not in excluded_credentials]

credential_dropdown = widgets.Dropdown(
    options=filtered_credentials,
    description=RIDOH_CREDENTIAL_COLUMN_NAME + ':',
    value=ROLE_MD_DO
)

country_dropdown = widgets.Dropdown(
    options=[ALL_VALUE] + combined['Institution_Country'].unique().tolist(),  # Add 'All' option
    description='Country:',
    value=ALL_VALUE  # Default value is 'All'
)

specialty_dropdown = widgets.Dropdown(
    options=[ALL_VALUE] + combined[SPECIALTY_COLUMN_NAME].unique().tolist(),  # Add 'All' option
    description= SPECIALTY_COLUMN_NAME + ':',
    value=ALL_VALUE  # Default value is 'All'
)

# Create a dropdown for number of slices
num_slices_dropdown = widgets.Dropdown(
    options=[3, 4, 5, 6, 7, 8, 9, 10],  # Options for number of slices
    description='Num Slices:',
    value=8  # Default value
)

# Output widget to hold pie charts
output_pie_charts = widgets.Output()




def aggregate_slices(df, column, num_slices=8):
    counts = df[column].value_counts()
    top_counts = counts.nlargest(num_slices - 1)
    other_count = counts[counts.index.isin(top_counts.index) == False].sum()
    
    # Create a new Series to hold the final counts, including "OTHER" if applicable
    aggregated_counts = top_counts.copy()
    if other_count > 0:
        aggregated_counts['OTHER'] = other_count
    
    # Sort the aggregated counts by value in descending order
    sorted_aggregated_counts = aggregated_counts.sort_values(ascending=False)
    
    return sorted_aggregated_counts.index, sorted_aggregated_counts.values

# Function to update pie charts based on selected credential, country, specialty, and num_slices
def update_pie_charts(selected_credential, selected_country, selected_specialty, num_slices):
    with output_pie_charts:
        output_pie_charts.clear_output()  # Clear the current output

        # Filter dataframe based on selected credential
        filtered_df = combined[combined[RIDOH_CREDENTIAL_COLUMN_NAME] == selected_credential]

        # Further filter by selected country if it's not 'All'
        if selected_country != ALL_VALUE:
            filtered_df = filtered_df[filtered_df['Institution_Country'] == selected_country]
        
        # Further filter by selected specialty if it's not 'All'
        if selected_specialty != ALL_VALUE:
            filtered_df = filtered_df[filtered_df[SPECIALTY_COLUMN_NAME] == selected_specialty]

        # Create pie charts while excluding "Unknown" for each column independently
        country_filtered = filtered_df[filtered_df['Institution_Country'] != "Unknown"]
        state_filtered = filtered_df[filtered_df['Institution_State'] != "Unknown"]
        region_filtered = filtered_df[filtered_df['Institution_Region_Census'] != "Unknown"]
        division_filtered = filtered_df[filtered_df['Institution_Division_Census'] != "Unknown"]
        decade_filtered = filtered_df[filtered_df['Graduation_Decade'] != "Unknown"]  # Exclude "Unknown"
        decade_filtered = decade_filtered[decade_filtered['Graduation_Decade'] != 'nan']

        # Aggregate slices and create pie charts
        country_names, country_values = aggregate_slices(country_filtered, 'Institution_Country', num_slices)
        state_names, state_values = aggregate_slices(state_filtered, 'Institution_State', num_slices)
        region_names, region_values = aggregate_slices(region_filtered, 'Institution_Region_Census', num_slices)
        division_names, division_values = aggregate_slices(division_filtered, 'Institution_Division_Census', num_slices)
        decade_names, decade_values = aggregate_slices(decade_filtered, 'Graduation_Decade', num_slices)  # New decade data

        # Create pie charts
        fig_country = go.FigureWidget(px.pie(names=country_names, values=country_values, title='Institution Country Breakdown', width=400, height=300))
        fig_state = go.FigureWidget(px.pie(
            names=state_names, 
            values=state_values, 
            title='Institution State Breakdown (if USA)', 
            width=400, 
            height=300,
            color_discrete_sequence=get_colors(state_names, state_color_mapping) # ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#f0eded']
        ))
        fig_region = go.FigureWidget(px.pie(names=region_names, values=region_values, title='Institution Region Breakdown (if USA)', width=400, height=300))
        fig_division = go.FigureWidget(px.pie(names=division_names, values=division_values, title='Institution Division Breakdown (if USA)', width=400, height=300))
        fig_decade = go.FigureWidget(px.pie(
            names=decade_names,
            values=decade_values,
            title='Graduation Decade Breakdown',
            width=400,
            height=300,
            color_discrete_sequence=get_colors(decade_names, decade_color_mapping)
        ))

        # Display charts inline using HBox
        display(widgets.HBox([fig_country, fig_decade]))
        display(widgets.HBox([fig_state, fig_region, fig_division, ]))

# Callback function when a dropdown value changes
def on_dropdown_change(change):
    update_pie_charts(credential_dropdown.value, country_dropdown.value, specialty_dropdown.value, num_slices_dropdown.value)

# Observe changes in all dropdowns
credential_dropdown.observe(on_dropdown_change, names='value')
country_dropdown.observe(on_dropdown_change, names='value')
specialty_dropdown.observe(on_dropdown_change, names='value')
num_slices_dropdown.observe(on_dropdown_change, names='value')

# Display the dropdowns and output widget
display(widgets.HBox([credential_dropdown, country_dropdown, specialty_dropdown, num_slices_dropdown]))
display(output_pie_charts)

# Initial pie charts for the default dropdown values
update_pie_charts(credential_dropdown.value, country_dropdown.value, specialty_dropdown.value, num_slices_dropdown.value)


# %% [markdown]
# ### Analysis of Clinician Productivity including bins/distributions

# %%
final_provider_list.loc[:, 'Total_Distinct_Medical_Claim_Count_PC_EQ'] = round(final_provider_list['APCD_CORE_PC_CLAIMS_COUNT'] / 900,1)
final_provider_list.loc[:, 'APCD_TOTAL_CLAIMS_ALL_EQ'] = round(final_provider_list[APCD_TOTAL_CLAIMS_ALL_COL_NAME] / 3360,1)

final_provider_list[SPECIALTY_COLUMN_NAME] = final_provider_list[SPECIALTY_COLUMN_NAME].replace('', 'Undetermined')  
final_provider_list[SPECIALTY_COLUMN_NAME] = final_provider_list[SPECIALTY_COLUMN_NAME].fillna('Undetermined')  


final_provider_list['Provider Gender Code'] = final_provider_list[NPPES_PREFIX + 'Provider Gender Code'].replace('', 'Unknown')  
final_provider_list['Provider Gender Code'] = final_provider_list[NPPES_PREFIX + 'Provider Gender Code'].fillna('Unknown')  

physicians = final_provider_list[final_provider_list[RIDOH_CREDENTIAL_COLUMN_NAME] == 'Physician']

group_by_dropdown = widgets.Dropdown(
    options=['Specialty', 'Specialty and Gender'],
    description='Group by:',
    value='Specialty and Gender',  # Default value
)

pc_to_fte_division_factor = widgets.IntText(
    value=900,  # Default value
    step=10,    # Step size
    layout=widgets.Layout(width='100px')
)

pc_to_fte_division_factor_label = widgets.Label(
    value='# PC Claims For 1 FTE:',
    layout=widgets.Layout(width='150px', height='30px', display='flex', align_items='center')
)

all_to_fte_division_factor = widgets.IntText(
    value=3360,  # Default value
    step=50,    # Step size
    layout=widgets.Layout(width='100px')
)

all_to_fte_division_factor_label = widgets.Label(
    value='# All Claims For 1 FTE:',
    layout=widgets.Layout(width='150px', height='30px', display='flex', align_items='center')
)

controls = widgets.HBox([group_by_dropdown, pc_to_fte_division_factor_label, pc_to_fte_division_factor, all_to_fte_division_factor_label, all_to_fte_division_factor])


def display_grouped(group_by, pc_to_fte_division_factor, all_to_fte_division_factor):
    grouping_keys = [SPECIALTY_COLUMN_NAME]
    if group_by == 'Specialty and Gender':
        grouping_keys.append('Provider Gender Code')

    grouped_df = physicians.groupby(grouping_keys).agg(
            Total_PC_Claims=('APCD_CORE_PC_CLAIMS_COUNT', 'sum'),
            Total_All_Claims=(APCD_TOTAL_CLAIMS_ALL_COL_NAME, 'sum'),
            Number_Providers=(SPECIALTY_COLUMN_NAME, 'size'),
        ).reset_index()
    
    grouped_df = grouped_df.rename(columns={
        'Provider Gender Code': 'Gender',
        'Total_PC_Claims': 'Total Primary Care Claims',
        'Total_All_Claims': 'Total Claims',
        'Number_Providers': 'Number of Providers'
    })

    total_row_values = grouped_df[['Total Primary Care Claims', 'Total Claims', 'Number of Providers']].sum().tolist()
    if group_by == 'Specialty and Gender':
        total_row_values.insert(0, '') 

    total_row = pd.DataFrame([['Total'] + total_row_values], columns=grouped_df.columns)
    grouped_df = pd.concat([grouped_df, total_row], ignore_index=True)

    if pc_to_fte_division_factor > 0:  # Ensure division factor is greater than zero to avoid division by zero
        grouped_df['Full Equiv. by Primary Care Claims'] = grouped_df['Total Primary Care Claims'] / pc_to_fte_division_factor

    if all_to_fte_division_factor > 0:
        grouped_df['Full Equiv. by All Claims'] = grouped_df['Total Claims'] / all_to_fte_division_factor


    def highlight_total(s):
        is_total_row = s.iloc[0] == 'Total'
        return ['font-weight: bold; border: 2px solid black;background-color: lightblue' if is_total_row else '' for _ in s]
    
    styled_df = grouped_df.style.apply(highlight_total, axis=1) \
        .format("{:.1f}", subset=['Total Primary Care Claims', 'Total Claims', 'Number of Providers', 'Full Equiv. by Primary Care Claims','Full Equiv. by All Claims']) 
    
    display(styled_df)

output = widgets.interactive_output(display_grouped, {
    'group_by': group_by_dropdown,
    'pc_to_fte_division_factor': pc_to_fte_division_factor,
    'all_to_fte_division_factor':all_to_fte_division_factor,
})

display(controls, output)

# %%
print("There are many ways to think about the patient panel size for these physicians.\n")

print("By looking at primary care claims:\n")
print("When considering 1 year number of members by just primary care claims: ", physicians['APCD_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT'].mean())
print("When considering 1 year number of members by just primary care claims (looking at internal id): ", physicians['APCD_INTERNAL_MEMBER_ID_CORE_PC_ONE_YEAR_COUNT'].mean())
print("When considering 2 year number of members by just primary care claims: ", physicians['APCD_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT'].mean())
print("When considering 2 year number of members by just primary care claims(looking at internal id): ", physicians['APCD_INTERNAL_MEMBER_ID_CORE_PC_TWO_YEAR_COUNT'].mean())
print("For context, the average number of primary care claims over 1 year was: ", physicians['APCD_CORE_PC_CLAIMS_COUNT'].mean())
print("-------------------------------------------------------------")

print("By looking at all claims:\n")
print("When considering 1 year number of members by all claims: ", physicians['APCD_MEMBER_ID_ALL_COUNT'].mean())
print("When considering 1 year number of members by all claims (looking at internal id): ", physicians['APCD_INTERNAL_MEMBER_ID_ALL_COUNT'].mean())

print("When considering 2 year number of members by all claims: ", physicians['APCD_MEMBER_ID_ALL_TWO_YEAR_COUNT'].mean())
print("When considering 2 year number of members by all claims (looking at internal id): ", physicians['APCD_INTERNAL_MEMBER_ID_ALL_TWO_YEAR_COUNT'].mean())

# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from ipywidgets import interact, SelectMultiple

bin_edges = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, np.inf]
bin_labels = ['0-0.1', '0.1-0.2', '0.2-0.3', '0.3-0.4', '0.4-0.5', '0.5-0.6', '0.6-0.7', '0.7-0.8', '0.8-0.9', '0.9+']
physicians.loc[:, 'Bin'] = pd.cut(physicians['Total_Distinct_Medical_Claim_Count_PC_EQ'], bins=bin_edges, labels=bin_labels, right=False)

def plot_histogram(specialties=None):
    # Filter by selected specialties if any are provided
    if 'None' not in specialties:
        df_filtered = physicians[physicians[SPECIALTY_COLUMN_NAME].isin(specialties)]
    else:
        df_filtered = physicians  # No specialty selected, plot entire dataset

    bin_counts = df_filtered['Bin'].value_counts(sort=False)
    
    # Plotting
    plt.figure(figsize=(10, 6))
    bin_counts.plot(kind='bar', edgecolor='black')
    
    # Customizing the plot
    plt.title('Distribution of Total Distinct Medical Claim Count by Bin for Specialty: ' + str(specialties[0]))
    plt.xlabel('Bins')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    
    plt.show()

specialties = ['None'] + physicians[SPECIALTY_COLUMN_NAME].unique().tolist()
interact(plot_histogram, specialties=SelectMultiple(options=specialties, description='Specialties', value=('None',)))

# %%
print("To summarize:")
print("------------------------------------")
print(final_provider_list[RIDOH_CREDENTIAL_COLUMN_NAME].value_counts())
print("------------------------------------")
print(final_provider_list[SPECIALTY_COLUMN_NAME].value_counts())
print("Physician FTE equivalents (by PC claims):", final_provider_list[final_provider_list[RIDOH_CREDENTIAL_COLUMN_NAME] == 'Physician']['APCD_CORE_PC_CLAIMS_COUNT'].sum() / (900))
print("Physician FTE equivalents (by PC claims):", final_provider_list[final_provider_list[RIDOH_CREDENTIAL_COLUMN_NAME] == 'Physician'][APCD_TOTAL_CLAIMS_ALL_COL_NAME].sum() / (3360))

# %% [markdown]
# # Additional Datasources to consider integrating:
# 
# https://data.cms.gov/tools/medicare-revalidation-list/provider/O20040203000479 
# https://data.cms.gov/api-docs 

# %% [markdown]
# ## Scratch Sanity Check Analysis
# ``` sql
# SELECT Rendering_Provider_NPI, COUNT(*) FROM medical_claim mc
# WHERE Rendering_Provider_NPI IN (
#     '1306147111', -- Concentra Urgent Care
#     '1649511841', -- Minute Clinic 
#     '1316402746', -- Minute Clinic
#     '1134191836', -- Minute Clinic
#     '1457470858', -- MinuteClinic
#     '1376891416', -- Carewell Urgent Care
#     '1528221470', -- NORTH PROVIDENCE URGENT CARE 
#     '1568555183', -- SCMG EXPRESS CARE EAST GREENWICH
#     '1437119005' -- Lincoln Urgent Care Center
# )
# AND mc.first_service_dt >= '2022-06-01'
# AND mc.procedure_code IN ('G0402', 'G0438', 'G0439', '99381', '99382', '99383', '99384', '99385', '99386', '99387', '99391', '99392', '99393', '99394', '99395', '99396', '99397', '90471')
# GROUP BY Rendering_Provider_NPI
# 
# 
# -- Attending_Internal_Provider_Id, Billing_Internal_Provider_Id, Ordering_Internal_Provider_Id, Operating_Internal_Provider_Id, Other_Internal_Provider_Id, Referring_Internal_Provier_Id 
# SELECT Attending_Internal_Provider_Id, Rendering_Provider_NPI, COUNT(*) FROM medical_claim mc
# WHERE Attending_Internal_Provider_Id IN (
#     SELECT Attending_Internal_Provider_Id FROM Provider_Master where NPI IN (
#         '1306147111', -- Concentra Urgent Care -- 10484848 
#         '1649511841', -- Minute Clinic -- 12509669
#         '1316402746', -- Minute Clinic -- 12509707
#         '1134191836', -- Minute Clinic -- 7950009
#         '1457470858', -- MinuteClinic -- 1018675
#         '1376891416', -- Carewell Urgent Care -- 1069741
#         '1528221470', -- NORTH PROVIDENCE URGENT CARE -- 9149369
#         '1568555183', -- SCMG EXPRESS CARE EAST GREENWICH -- 11531596
#         '1437119005' -- Lincoln Urgent Care Center -- 11531596
#     ) 
# ) 
# AND Attending_Internal_Provider_Id <> '-1'
# AND mc.first_service_dt >= '2022-06-01'
# AND mc.procedure_code IN ('G0402', 'G0438', 'G0439', '99381', '99382', '99383', '99384', '99385', '99386', '99387', '99391',
#  '99392', '99393', '99394', '99395', '99396', '99397', '90471')
# GROUP BY Attending_Internal_Provider_Id, Rendering_Provider_NPI
# ```
#     
# 


