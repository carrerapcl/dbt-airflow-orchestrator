import yaml
import os

class bcolors:
    TITLE = '\033[95m'
    OKGREEN = '\033[92m'
    OKBLUE = '\033[94m'
    WARN = '\033[93m'
    FAIL = '\033[91m'
    COMMENT = '\033[90m'
    ENDC = '\033[0m'

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

def get_default_config_path():
    """Returns the default path for the configuration file."""
    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(current_script_dir, '..', '..', 'config.yaml')

def load_config(config_path='config.yaml'):
    """Load the configuration file and return the configuration data."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise ConfigError(f"{bcolors.FAIL}Configuration file not found: {config_path}{bcolors.ENDC}")
    except yaml.YAMLError as exc:
        raise ConfigError(f"{bcolors.FAIL}Error parsing YAML configuration: {exc}{bcolors.ENDC}")

def get_config_value(config, section, key):
    """Retrieve a value from the configuration and raise an error if not found."""
    try:
        return config[section][key]
    except KeyError:
        raise ConfigError(f"{bcolors.FAIL}Missing '{key}' in '{section}' section of the configuration.{bcolors.ENDC}")
