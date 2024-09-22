def write_output(validation_Type, source, target, Number_of_source_Records, Number_of_target_Records,
                 Number_of_failed_Records, column, Status, source_type, target_type, Out):
    Out["source"].append(source)
    Out["target"].append(target)
    Out["column"].append(column)
    Out["validation_Type"].append(validation_Type)
    Out["number_of_source_Records"].append(Number_of_source_Records)
    Out["number_of_target_Records"].append(Number_of_target_Records)
    Out["status"].append(Status)
    Out["number_of_failed_Records"].append(Number_of_failed_Records)
    Out["source_type"].append(source_type)
    Out["target_type"].append(target_type)