from azure.loganalytics import LogAnalyticsDataClient
from azure.common.credentials import get_azure_cli_credentials
from azure.loganalytics.models import QueryBody
import pandas as pd

class LogAnalytics:
    def get_log_analytics(self):
        creds, _ = get_azure_cli_credentials(resource="https://api.loganalytics.io")
        log_client = LogAnalyticsDataClient(creds)
        myWorkSpaceId = '<Workspace_id>'
        result = log_client.query(myWorkSpaceId, QueryBody(**{'query': 'AzureDiagnostics | where TimeGenerated > ago(7d) | limit 10'}))
        cols = []
        for item in result.tables:
            col = item.columns
            for item in col:
                col_name = item.name
                cols.append(col_name)

        for item in result.tables:
            row = item.rows

        res_df = pd.DataFrame(row, columns = cols)
        # print(res_df)
        return res_df
#res_df.to_csv(r'C:\HadoopExam\log_analysis.csv', index = False, header=True)