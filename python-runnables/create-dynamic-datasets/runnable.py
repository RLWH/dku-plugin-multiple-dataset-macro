# This file is the actual code for the Python runnable create-dynamic-datasets
from dataiku.runnables import Runnable, ResultTable

# User import
import time
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

        self.num_files = self.config.get("num_files", "defaultValue")
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
        num_files = 

        