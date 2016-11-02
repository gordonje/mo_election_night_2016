import os
from lxml import etree

parser = etree.XMLParser(remove_blank_text=True)
path = 'archive_2014/'

for filename in os.listdir(path):
    if filename.endswith('.xml'):
        with open(os.path.join(path, filename), 'rb') as in_f:
            first_line = in_f.readline()
            in_f.seek(0)
            tree = etree.parse(in_f, parser)
            in_f.seek(5)
            with open(
                os.path.join(path, filename.replace('_', '%')),
                'wb'
            ) as f:
                f.write(first_line)
                tree.write(f, pretty_print=True)
