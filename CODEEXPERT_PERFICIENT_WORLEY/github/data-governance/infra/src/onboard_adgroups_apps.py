from helpers import idc_apps as idc

def main(event):
    if "version" in event and event["version"] == "2":
        try:
            idc.add_ad_groups_to_redshift_serverless_app()
            idc.add_serverless_ad_groups_to_quicksight()
        except Exception as e:
            print(e)
    # default is redshift version 1
    else:
        try:
            idc.add_ad_groups_to_redshift_app()
            idc.add_ad_groups_to_quicksight()
        except Exception as e:
            print(e)

def lambda_handler(event, context):
    main(event)

if __name__ == "__main__":
    event = {"version":"2"}
    main(event)