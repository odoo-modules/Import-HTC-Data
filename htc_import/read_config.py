import json
from os import getcwd, scandir, listdir
from os.path import join, isfile


class ReadConfig:

    def __init__(self):
        self.currentDirectory = getcwd()
        with open(join(self.currentDirectory, 'config.json')) as json_file:
            data = json.load(json_file)
            self.url = data['url']
            self.db = data['db']
            self.password = data['password']
            self.username = data['username']
            self.folder_path = data['folderPath']

    def get_source_folder(self):
        return self.folder_path + "Source"
    
    def get_root_folder(self):
        return self.folder_path
        