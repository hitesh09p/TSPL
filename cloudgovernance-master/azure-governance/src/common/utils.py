from src.azureservices.usermanagement.user_mgmt_test import UserMgmt
from src.azureservices.loganalytics.log_analytics_test import LogAnalytics
from src.azureservices.usagemanagement.usage_mgmt_test import ConsumeMgmt

UserMgmt = UserMgmt()
LogAnalytics = LogAnalytics()
ConsumeMgmt = ConsumeMgmt()

def execute_service(servicename:str):
    if servicename == "usermanagement":
        res_df = UserMgmt.get_user_mgmt()
        print(res_df)
    elif servicename == "loganalytics":
        res_df = LogAnalytics.get_log_analytics()
        print(res_df)
    elif servicename == "usagemanagement":
        ConsumeMgmt.get_consumption()
        print("====")

