import configparser
import os

class ConfigParser:
    def __init__(self, env):
        self.env = env
        self.path = os.path.split(os.path.realpath(__file__))[0] + f'/config_{self.env}.conf'
        self.config = configparser.ConfigParser()
        self.config.read(self.path)

class Config:
    env = os.environ.get('ENV', 'stg')
    config = ConfigParser(env).config