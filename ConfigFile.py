import ConfigParser

# Settings File Format:

# [SectionName]
# parameter=setting
# param2 = setting2

def get_config(filename):
	config = ConfigParser.RawConfigParser()
	try:
		config.read(filename)
		return config
	except Exception:
		print ("Error - Configuration File Not Found.")
	return None


def get_settings(filename, section):
	config = get_config(filename)
	if config is not None:
		settings = {}
		for item in config.items(section):
			settings[item[0]] = item[1]
		return settings
	return None


def set_property(filename, section, prop, val):
	config = get_config(filename)
	config.set(section, prop, val)
	with open(filename, 'wb') as configfile:
		config.write(configfile)
