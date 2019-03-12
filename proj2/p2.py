from google.cloud import bigquery
import pandas as pd

#Init
client = bigquery.Client()

hn_dataset_ref = client.dataset('cms_medicare', project='bigquery-public-data')
hn_dset = client.get_dataset(hn_dataset_ref)
hn_full = client.get_table(hn_dset.table('hospital_general_info'))

#
def estimate_gigabytes_scanned(query, bq_client):
    # see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.dryRun
    my_job_config = bigquery.job.QueryJobConfig()
    my_job_config.dry_run = True
    my_job = bq_client.query(query)

    return my_job

def create_query_string(year):
    #
    query = "SELECT zip_code, hospital_type, emergency_services, hospital_overall_rating, mortality_national_comparison,readmission_national_comparison, patient_experience_national_comparison, effectiveness_of_care_national_comparison, efficient_use_of_medical_imaging_national_comparison, drg_definition, total_discharges, average_covered_charges, average_total_payments, average_medicare_payments, hospital_ownership FROM `bigquery-public-data.cms_medicare.inpatient_charges_" + year + "` as inpatient_2011 JOIN `bigquery-public-data.cms_medicare.hospital_general_info` as hos_gen ON hos_gen.provider_id = inpatient_2011.provider_id"
    return query

twenty11 = estimate_gigabytes_scanned(create_query_string("2011"), client)
twenty12 = estimate_gigabytes_scanned(create_query_string("2012"), client)
twenty13 = estimate_gigabytes_scanned(create_query_string("2013"), client)
twenty14 = estimate_gigabytes_scanned(create_query_string("2014"), client)
twenty15 = estimate_gigabytes_scanned(create_query_string("2015"), client)

df1 = twenty11.to_dataframe()
df2 = twenty12.to_dataframe()
df3 = twenty13.to_dataframe()
df4 = twenty14.to_dataframe()
df5 = twenty15.to_dataframe()

frames = [df1, df2, df3, df4, df5]

results = pd.concat(frames)
hundy = results.sample(n=100000)
hundy.to_csv("hospital.csv")
