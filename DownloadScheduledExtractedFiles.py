import DSSClient

dss_username = None
dss_password = None
schedule_name = None
file_type = "all"
aws_download = False
dss_client  = DSSClient.DSSClient()


def show_all_schedule(schedules):
    if schedules != None:
        if "value" in schedules:
            for schedule in schedules["value"]:
                if schedule["Trigger"]["@odata.type"] != "#DataScope.Select.Api.Extractions.Schedules.ImmediateTrigger":
                    print("-\t ",schedule["Name"], ":", schedule["Trigger"]["@odata.type"])

if __name__ == "__main__":
    
    dss_username = "9034508"
    dss_password = "Refinitiv1969!"
    schedule_name = "EOD Bond Analytics Daily Test"

    #The Process is:
    #   1. Logging via basic authentication
    #   2. Get schedule by name
    #   3. Get last instance of extracted data
    #   4. Extract file
    #   5. Write locally

    try:
        dss_client.login(dss_username,dss_password)
        print("Login succeeded")
        if schedule_name == None:
            schedules = dss_client.list_all_schedules()
            show_all_schedule(schedules)
        else:
            schedule = dss_client.get_schedule_by_name(schedule_name)
            print("\nThe schedule ID of", schedule["Name"], "is", schedule["ScheduleId"], "(",schedule["Trigger"]["@odata.type"],")");
            last_report_extraction = dss_client.get_last_extraction(schedule)
            print("\nThe last extraction was extracted on", last_report_extraction["ExtractionDateUtc"],"GMT");
            
            if file_type == "all":
                extracted_files = dss_client.get_all_files(last_report_extraction)
                if "value" in extracted_files:
                    for file in extracted_files["value"]:                      
                        print("\n"+file["ExtractedFileName"], "(", file["Size"],"bytes) is available on the server.")
                        print("\nDownloading...")
                        dss_client.download_file(file, aws_download)
                        print("\nDownload Completed")
            else:
                extracted_file = dss_client.get_file(last_report_extraction, file_type)
                print("\n"+extracted_file["ExtractedFileName"], "(", extracted_file["Size"],"bytes) is available on the server.")
                print("\nDownloading...")
                dss_client.download_file(extracted_file, aws_download)
                print("\nDownload Completed")

    except Exception as ex:     
        print("Exception", ex)