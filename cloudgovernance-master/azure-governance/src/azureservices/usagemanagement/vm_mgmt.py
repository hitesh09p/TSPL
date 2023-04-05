from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
import datetime
import pandas as pd
from azure.mgmt.advisor import AdvisorManagementClient



main_df = pd.DataFrame()

SUBSCRIPTION_ID = '<SUBSCRIPTION-ID>'
#GROUP_NAME = 'exeliq'
#LOCATION = 'westus'
#VM_NAME = 'zeppelin-nifi-setup'

credentials = ServicePrincipalCredentials(
    client_id='<client-id>',
    secret='<secrete-id>',
    tenant='<tenant-id>'
    )
client = MonitorManagementClient(
    credentials,
    SUBSCRIPTION_ID
)

today = datetime.datetime.now().date()
yesterday = today - datetime.timedelta(days=30)
compute_client = ComputeManagementClient(credentials,SUBSCRIPTION_ID)

vm_list = compute_client.virtual_machines.list_all()
# vm_list = compute_client.virtual_machines.list('resource_group_name')
#i= 0


for vm in vm_list:
    array = vm.id.split("/")
    resource_group = array[4]
    vm_name = array[-1]


    resource_id =  (
        "subscriptions/{}/"
        "resourceGroups/{}/"
        "providers/Microsoft.Compute/virtualMachines/{}"
    ).format(SUBSCRIPTION_ID, resource_group, vm_name)

    metrics_data = client.metrics.list(
        resource_id,
        timespan="{}/{}".format(yesterday, today),
        interval='PT24H',
        metricnames='Percentage CPU',
        aggregation='average'

    )

    for item in metrics_data.value:
        dfindex_count = 0
        weekday_counter = 0
        temp_df = pd.DataFrame()
        weekly_list = []
        weekdate_list =[]
        monthdate_list = []
        monthly_average_list = []
        weekly_counter = 1
        for timeserie in item.timeseries:
            for data in timeserie.data:
                if data.average == None:
                    data.average=0

                #monthly avg
                monthly_average_list.append(data.average)
                monthdate_list.append(data.time_stamp)
            monthly_avg = sum(monthly_average_list)/ len(monthly_average_list)
            if monthly_avg >=5:
                temp_df.loc[dfindex_count, "subscription_id"] = SUBSCRIPTION_ID
                temp_df.loc[dfindex_count, "vm_name"] = vm_name
                temp_df.loc[dfindex_count, "aggregate_by"] = "Monthly"
                temp_df.loc[dfindex_count, "start_date"] = monthdate_list[0]
                temp_df.loc[dfindex_count, "end_date"] = monthdate_list[-1]
                temp_df.loc[dfindex_count, "CPU_Usage"] = monthly_avg
                dfindex_count= dfindex_count+ 1
                # print("monthly avg",monthly_avg)
                # print("resource group name", resource_group)
                # print("vm name", vm_name)



            for index, d_avg in enumerate(monthly_average_list):
                # weekly avg

                if weekday_counter == 7:
                    weekly_avg = sum(weekly_list) / len(weekly_list)
                    # if weekly_avg<5:
                    if monthly_avg < 5 and monthly_avg > 2:
                        temp_df.loc[dfindex_count, "subscription_id"] = SUBSCRIPTION_ID
                        temp_df.loc[dfindex_count, "vm_name"] = vm_name
                        temp_df.loc[dfindex_count, "aggregate_by"] = "Week" + str(weekly_counter)
                        temp_df.loc[dfindex_count, "start_date"] = weekdate_list[0]
                        temp_df.loc[dfindex_count, "end_date"] = weekdate_list[-1]
                        temp_df.loc[dfindex_count, "CPU_Usage"] = weekly_avg
                        dfindex_count= dfindex_count+ 1
                        # print("week"+str(weekly_counter), weekly_avg)
                        # print("resource group name", resource_group)
                        # print("vm name", vm_name)
                    weekly_list = []
                    weekdate_list = []
                    weekday_counter = 0
                    weekly_list.append(monthly_average_list[index])
                    weekdate_list.append(monthdate_list[index])
                    weekly_counter = weekly_counter + 1
                else:
                    weekly_list.append(monthly_average_list[index])
                    weekdate_list.append(monthdate_list[index])
                #daily avg
                if monthly_avg <= 2:
                    temp_df.loc[dfindex_count, "subscription_id"] = SUBSCRIPTION_ID
                    temp_df.loc[dfindex_count, "vm_name"] = vm_name
                    temp_df.loc[dfindex_count, "aggregate_by"] = "Daily"
                    temp_df.loc[dfindex_count, "start_date"] = monthdate_list[index]
                    endtime = monthdate_list[index] + datetime.timedelta(1)
                    temp_df.loc[dfindex_count, "end_date"] = endtime
                    temp_df.loc[dfindex_count, "CPU_Usage"] = d_avg

                    dfindex_count= dfindex_count+ 1
                    # print("{},{} daily avg".format(data.time_stamp, data.average))
                # print("resource group name", resource_group)
                # print("vm name", vm_name)
                weekday_counter = weekday_counter + 1


    main_df = main_df.append(temp_df, ignore_index=True)
main_df.rename(columns={'vm_name':'Resource Name'}, inplace=True)
#main_df.to_csv("D:/sample_df_3.csv",index=False)


aClient = AdvisorManagementClient(credentials, SUBSCRIPTION_ID)
#for recommendation in aClient.recommendations.list():
 #   print(recommendation.impacted_value)

adv_client_list = aClient.recommendations.list()

l = []

for item in adv_client_list:
     category = item.category
     business_impact = item.impact
     recommendation = item.short_description.solution
     id_array = item.id.split("/")
     subcription_id = id_array[2]
     resource_group = id_array[4]
     resource_name = item.impacted_value
     type_array = item.impacted_field.split("/")
     resource_type = type_array[1]
     l.append(category + "|" + business_impact + "|" + recommendation + "|" + subcription_id + "|" + resource_group + "|" + resource_name + "|" + resource_type)

res_items = []
for item in l:
    res_items.append(item.split('|'))

res_df = pd.DataFrame(res_items, columns=["Category", "Business Impact", "Recommendation", "Subscription ID", "Resource Group", "Resource Name", "Resource Type"])
#res_df.to_csv("D:/advisor_extract1.csv", index=False)


df_merge = main_df.merge(res_df, how='inner', on='Resource Name')

df_merge.to_csv("D:/consolidated_report.csv", index=False)






















