import pprint
from v7_access.v7api import dataset

# result = dataset.create_dataset('test_dataset')
# pprint.pprint(result)
# dataset.add_labels_to_dataset('test_dataset', ['d', 'e', 'f'], 'tag')
# result = dataset.get_annotations('20201215-fbumper-grouped-2-1-me', anno_dest_dir='annos')
# pprint.pprint(result)
result = dataset.get_dataset_files('20201215-fbumper-grouped-2-1-me')
pprint.pprint(list(result))
