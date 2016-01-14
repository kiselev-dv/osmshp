import yaml
import os

class YAMLLoader(yaml.Loader):

    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(YAMLLoader, self).__init__(stream)

    def include(self, node):
        filename = self.construct_scalar(node)
        if not filename.startswith('/'):
            filename = os.path.join(self._root, filename)
        with open(filename, 'r') as fp:
            return yaml.load(fp, YAMLLoader)

    def path(self, node):
        filename = self.construct_scalar(node)
        if not filename.startswith('/'):
            filename = os.path.abspath(os.path.join(self._root, filename))
        return filename


YAMLLoader.add_constructor('!include', YAMLLoader.include)
YAMLLoader.add_constructor('!path', YAMLLoader.path)