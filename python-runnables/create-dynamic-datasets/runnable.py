# This file is the actual code for the Python runnable create-dynamic-datasets
from dataiku.runnables import Runnable, ResultTable

# User import
import time
import datetime
import dataiku
import uuid
import random
import pandas as pd

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config

        # Obtain configuration variables
        self.num_files = self.config.get("num_datasets", "defaultValue")
        self.seed = self.config.get("seed", "defaultValue")
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return (self.num_files, "FILES")

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        
        # Get project instance here
        client = dataiku.api_client()
        project = client.get_project(self.project_key)

        def update_percent(percent, last_update_time):
            """
            Update progress bar
            """

            new_time = time.time()
            if (new_time - last_update_time) > 3:
                progress_callback(percent)
                return new_time
            else:
                return last_update_time

        # A boolean used to provide an informative message to the user when the macro creates a dataset
        macro_creates_dataset = False

        # List the datasets in the project
        datasets_in_project = []
        for dataset in project.list_datasets():
            datasets_in_project.append(dataset.get('name'))

        # Actions performed - State dictionary
        actions_performed = dict()
        num_files = int(self.num_files)
        seed = int(self.seed)
        
        print("Number of files: ", num_files)
        print(seed)

        random.seed(seed)

        update_time = time.time()

        for i in range(num_files):
            
            print("File ", i, " is creating")
            
            # Dataset name
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            dataset_name = f"dataset-{i}-{timestamp}"

            actions_performed[dataset_name] = "created"

            # Core logic here
# #             dataset = project.create_s3_dataset(dataset_name, "dataiku-managed-storage", "dynamic_dataset")
#             builder = project.new_managed_dataset(dataset_name)
#             builder.with_store_into("dataiku-managed-storage", format_option_id="S3")
#             dataset = builder.create(overwrite=True)
            
#             #setup format & schema  settings
#             ds_settings = dataset.get_settings()
# #             ds_settings.set_format("csv")
#             ds_settings.set_csv_format()
#             ds_settings.add_raw_schema_column({'name':'id', 'type':'string'})
#             ds_settings.add_raw_schema_column({'name':'value', 'type':'float'})
#             ds_settings.save()
            dataset = project.create_upload_dataset(dataset_name) # you can add connection= for the target connection
    
    
            df = pd.DataFrame({
                'id': [uuid.uuid4() for _ in range(10)],
                'value': [random.random() for _ in range(10)]
            })
        
            df.to_csv(f"{dataset_name}.csv", index=False)

            with open(f"{dataset_name}.csv", "rb") as f:
                    dataset.uploaded_add_file(f, f"{dataset_name}.csv")

            # At this point, the dataset object has been initialized, but the format is still unknown, and the
            # schema is empty, so the dataset is not yet usable

            # We run autodetection
            settings = dataset.autodetect_settings()
            # settings is now an object containing the "suggested" new dataset settings, including the detected format
            # andcompleted schema
            # We can just save the new settings in order to "accept the suggestion"
            settings.save()


            with dataset.get_as_core_dataset().get_writer() as writer:
                for idx, row in df.iterrows():
                    writer.write_row_array(row)

            percent = 100 * float(i+1) / num_files
            update_time = update_percent(percent, update_time)
            
            
            
            time.sleep(1)

        macro_creates_dataset = True

        # Output table
        rt = ResultTable()
        rt.add_column("actions", "Actions", "STRING")

        # Actions : "dataset" has been created or replaced
        for i in range(len(actions_performed)):
            record = []
            record.append(list(actions_performed.keys())[i] + " has been " + list(actions_performed.values())[i])
            rt.add_record(record)

        if macro_creates_dataset:
            rt.add_record(["Please refresh this page to see new datasets."])

        return rt


        