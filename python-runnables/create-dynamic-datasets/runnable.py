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
        return (2, "FILES")

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
        
        print(num_files)
        print(seed)

        random.seed(seed)

        update_time = time.time()

        for i in range(num_files):
            
            # Dataset name
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            dataset_name = f"dataset-{timestamp}"

            actions_performed[dataset_name] = "created"

            # Core logic here
            builder = project.new_managed_dataset(dataset_name)
#             builder.with_store_into("filesystem_folders")
            dataset = builder.create(overwrite=True)

            df = pd.DataFrame({
                'id': [uuid.uuid4() for _ in range(10)],
                'value': [random.random() for _ in range(10)]
            })

            dataset.set_schema({'columns': [{'name': column, 'type': 'string'} for column, column_type in df.dtypes.items()]})

            with dataset.get_as_core_dataset().get_writer() as writer:
                writer.write_dataframe(df)

            percent = 100 * float(i+1)/num_files
            update_time = update_percent(percent, update_time)

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


        