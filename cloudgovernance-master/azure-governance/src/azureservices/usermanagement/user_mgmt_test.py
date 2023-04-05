from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.cli.core import get_default_cli
import pandas as pd

class UserMgmt():

    def get_user_mgmt(self):
        # need to pass it from yml or parameter file
        credentials = ServicePrincipalCredentials(
            client_id='<client-id>',
            secret='<secrete-id>',
            tenant='<tenant-id>'
        )
        sub_client = SubscriptionClient(credentials)

        l = []
        for sub in sub_client.subscriptions.list():
           rs_client = ResourceManagementClient(credentials, sub.subscription_id)
           for res_grp in rs_client.resource_groups.list():
               res_grp = res_grp.name
               for resource in rs_client.resources.list_by_resource_group(res_grp):
                   res_grp = res_grp
                   resource_name = resource.name
                   role_assign = self.az_cli_test('role assignment list -o json')
                   for item in role_assign:
                          principalName = item['principalName']
                          principalType = item['principalType']
                          roleDefinitionName = item['roleDefinitionName']
                          scope = item['scope']
                          l.append(sub.subscription_id + "," + res_grp + "," + resource_name + "," +
                                   principalType + "," +principalName + "," + roleDefinitionName + "," + scope)

        res_items = []
        for item in l:
            res_items.append(item.split(','))

        res_df = pd.DataFrame(res_items, columns = ['subscription_id', 'resource_grp',
                            "resource_name", "principalType", "principalName", "roleDefinitionName", "scope"])

        #print(res_df)
        #res_df.to_csv(r'C:\HadoopExam\export_result1.csv', index = False, header=True)
        return res_df

    def az_cli_test(self, args_str):
        args = args_str.split()
        cli = get_default_cli()
        cli.invoke(args)
        if cli.result.result:
            return cli.result.result
        elif cli.result.error:
            raise cli.result.error

