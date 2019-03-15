#!/usr/bin/env python
# coding: utf-8

# In[30]:



import numpy as np
import pandas as pd
pd.options.display.max_colwidth = 100
import matplotlib.pyplot

from google.cloud import bigquery #For BigQu


# In[31]:


#
def estimate_gigabytes_scanned(query, bq_client):
    # see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.dryRun
    my_job_config = bigquery.job.QueryJobConfig()
    my_job_config.dry_run = True
    my_job = bq_client.query(query)

    return my_job


# In[32]:


#Init
client = bigquery.Client()

hn_dataset_ref = client.dataset('nhtsa_traffic_fatalities', project='bigquery-public-data')
hn_dset = client.get_dataset(hn_dataset_ref)
hn_full = client.get_table(hn_dset.table('accident_2015'))


# In[33]:


accidents_query_2015_all = """SELECT month_of_crash,
                                 day_of_week,
                                 hour_of_crash,
                                 manner_of_collision_name,
                                 light_condition_name,
                                 land_use_name,
                                 latitude,
                                 longitude,
                                 atmospheric_conditions_1_name,
                                 number_of_drunk_drivers,
                                 number_of_fatalities
                          FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
                          WHERE longitude < 0
                          AND longitude > -140
                      """

accidents_query_2016_all = """SELECT month_of_crash,
                                 day_of_week,
                                 hour_of_crash,
                                 manner_of_collision_name,
                                 light_condition_name,
                                 land_use_name,
                                 latitude,
                                 longitude,
                                 atmospheric_conditions_1_name,
                                 number_of_drunk_drivers,
                                 number_of_fatalities
                          FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2016`
                          WHERE longitude < 0
                          AND longitude > -140
                      """

accidents_query_multiple_2015 = """SELECT month_of_crash,
                                          day_of_week,
                                          hour_of_crash,
                                          manner_of_collision_name,
                                          light_condition_name,
                                          land_use_name,
                                          latitude,
                                          longitude,
                                          atmospheric_conditions_1_name,
                                          number_of_drunk_drivers,
                                          number_of_fatalities
                                    FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
                                    WHERE number_of_fatalities > 1
                                    AND longitude < 0
                                    AND longitude > -140
                      """

accidents_query_multiple_2016 = """SELECT month_of_crash,
                                          day_of_week,
                                          hour_of_crash,
                                          manner_of_collision_name,
                                          light_condition_name,
                                          land_use_name,
                                          latitude,
                                          longitude,
                                          atmospheric_conditions_1_name,
                                          number_of_drunk_drivers,
                                          number_of_fatalities
                                    FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2016`
                                    WHERE number_of_fatalities > 1
                                    AND longitude < 0
                                    AND longitude > -140
                      """


# In[34]:


accidents_2015 = estimate_gigabytes_scanned(accidents_query_2015_all, client)
accidents_2016 = estimate_gigabytes_scanned(accidents_query_2016_all, client)


# In[38]:


df15 = accidents_2015.to_dataframe()
df16 = accidents_2016.to_dataframe()
# print(df16)
#
frames = [df15, df16]
results = pd.concat(frames)


# In[45]:



imbill = results.loc[0, "manner_of_collision_name"]
print(imbill)


# In[ ]:


#hundy = results.sample(n=100000)
results.to_csv("hightway_fatals.csv")
