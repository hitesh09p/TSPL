from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.consumption import ConsumptionManagementClient
from azure.mgmt.advisor import AdvisorManagementClient
from azure.mgmt.subscription import SubscriptionClient
from azure.cli.core import get_default_cli
import pandas as pd

class ConsumeMgmt():

    def get_consumption(self):
    # need to pass it from yml or parameter file
        credentials = ServicePrincipalCredentials(
            client_id='<client-id>',
            secret='<secrete-id>',
            tenant='<tenant-id>'
        )

        sub_client = SubscriptionClient(credentials)

        l = []
        for sub in sub_client.subscriptions.list():
            consume_client = ConsumptionManagementClient(credentials,  sub.subscription_id)
            Advisor_Client = AdvisorManagementClient(credentials,  sub.subscription_id)

            #Get advisor data:
            # for item in Advisor_Client.recommendations.list():
            #     print(item.short_description)

            #Get Usage data of resources:
            for item in consume_client.usage_details.list():
                 l.append( item.name + "|" + str(item.tags) + "|" +  item.billing_period_id + "|" + str(item.usage_start) +
                           "|" + str(item.usage_end) + "|" + item.instance_name + "|" + str(item.usage_quantity) + "|" + str(item.pretax_cost))

        res_items = []
        for item in l:
            res_items.append(item.split('|'))

        res_df = pd.DataFrame(res_items, columns=['res_name', 'res_tags',
                                                  "billing_period_id", "usage_start", "usage_end", "instance_name",
                                                  "usage_quantity", "pretax_cost"])

        # res_df['usage_start'] = pd.to_datetime(res_df['usage_start'])
        # res_df['usage_end'] = pd.to_datetime(res_df['usage_end'])
        #
        # res_df['usage_start'] = pd.to_datetime(res_df['usage_start'].astype(str).str[:-6])
        # res_df['usage_end'] = pd.to_datetime(res_df['usage_end'].astype(str).str[:-6])
        #
        # res_df['timedelta'] = res_df['usage_end'].values - res_df['usage_start']


        #res_df.to_csv(r'C:\HadoopExam\usage.csv', index = False, header=True)
