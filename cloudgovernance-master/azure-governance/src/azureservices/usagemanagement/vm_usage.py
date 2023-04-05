import pandas as pd
import datetime
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.monitor import MonitorManagementClient

# Get the ARM id of your resource. You might chose to do a "get"
# using the according management or to build the URL directly
# Example for a ARM VM
resource_id =  (
    "subscriptions/{}/"
    "resourceGroups/{}/"
    "providers/Microsoft.Compute/virtualMachines/{}"
).format('a0b97e47-6f3f-4696-a00c-a97b398209e0', 'exeliq', 'zeppelin-nifi-setup')


credentials = ServicePrincipalCredentials(
    client_id='<client-id>',
    secret='<secrete-id>',
    tenant='<tenant-id>'
    )
# create client
client = MonitorManagementClient(
    credentials,
    'a0b97e47-6f3f-4696-a00c-a97b398209e0'
)

today = datetime.datetime.now().date()
past7days = today - datetime.timedelta(days=7)

metrics_data = client.metrics.list(
    resource_id,
    timespan="{}/{}".format(past7days, today),
    interval='PT24H',
    metricnames='Percentage CPU',
    aggregation='Total'
)

l = []
for item in metrics_data.value:
    # azure.mgmt.monitor.models.Metric
    for timeserie in item.timeseries:
        for data in timeserie.data:
            # azure.mgmt.monitor.models.MetricData
            l.append(str(data.time_stamp) + ',' + str(data.total))

res_items = []

for item in l:
    res_items.append(item.split(','))

res_df = pd.DataFrame(res_items, columns=['Timestamp', 'percentage CPU',])
print(res_df)
#res_df.to_csv(r'C:\HadoopExam\cpu_usage.csv', index = False, header=True)
