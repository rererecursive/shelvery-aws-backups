
"""Load Shelvery's configuration.

Configuration values are loaded in the following order, with each later stage taking precedence:
	1. Default values
	2. Environment variables
	3. Lambda payload
	4. Resource tags
"""

import os
import yaml


class Config():
	def __init__(self, payload={}):
		self.load_defaults()
		self.load_environment_variables()
		self.load_lambda_payload(payload)

	def load_defaults(self):
		"""Parse the default configuration values from the YAML config file.
		"""
		path = os.path.join(os.path.dirname(__file__), 'defaults.yml')
		with open(path) as fh:
			self.config = yaml.safe_load(fh)

		print("Loaded config from defaults: %s" % (self.config))

	def load_environment_variables(self):
		"""Load any configuration values from environment variables.
		Overwrite the default values.
		"""
		overriden = self.load_items(os.environ.items())
		print ("Loaded config from environment variables: %s" % overriden)

	def load_lambda_payload(self, payload):
		"""Load any configuration values specified in the Lambda payload.

		Params:
			payload: a dictionary containing parameters.
		"""
		config = {}
		overriden = {}

		if 'config' in payload:
			config = payload['config'].items()
			overriden = self.load_items(config)

		print("Loaded config from Lambda payload: %s" % (overriden))

	def load_items(self, items):
		"""Load any items into the config.

		Params:
			items: a dictionary containing configuration values.
		Returns:
			a dictionary containing the overriden values.
		"""
		overriden = {}
		config_keys = list(self.config.keys())

		for k,v in items:
			kl = k.lower()
			for key in config_keys:
				if kl in self.config[key]:
					self.config[key][kl] = v
					overriden[k] = v

		return overriden
