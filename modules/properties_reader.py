# https://github.com/daryshtaev/properties_reader
# version 1.0.3

import jproperties
import os

class Properties:
    """Класс для инициализации файла свойств приложения (название файла по умолчанию - "app.properties") и обращения к параметрам приложения в нем."""
    def __init__(self, directory=os.path.dirname(os.path.realpath(__file__)), file="app.properties", encoding="utf-8"):
        self.directory = directory
        self.encoding = encoding
        self.file = file

    def get_property(self, name, default_value=None):
        configs_file = os.path.join(self.directory, self.file)
        configs = jproperties.Properties()
        with open(configs_file, 'rb') as read_prop:
            configs.load(read_prop, self.encoding)
        property_value = configs.get(name)
        if property_value is not None:
            property_value = property_value.data
        else:
            property_value = default_value
        return property_value
