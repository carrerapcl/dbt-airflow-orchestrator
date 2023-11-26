import yaml
import os

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

def get_default_config_path():
    """Returns the default path for the configuration file."""
    current_script_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(current_script_dir, '..', 'config.yaml')

def load_config(config_path='config.yaml'):
    """Load the configuration file and return the configuration data."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise ConfigError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as exc:
        raise ConfigError(f"Error parsing YAML configuration: {exc}")

def get_config_value(config, section, key):
    """Retrieve a value from the configuration and raise an error if not found."""
    try:
        return config[section][key]
    except KeyError:
        raise ConfigError(f"Missing '{key}' in '{section}' section of the configuration.")
