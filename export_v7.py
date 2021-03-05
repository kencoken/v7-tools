import pprint
from v7_access.v7api import dataset

experiment_slug = '20210119-fbumper-grouped-2-1-s1'

result = dataset.get_annotations(experiment_slug, anno_dest_dir='annos',
                                 clear_directory=True, verbose=True)
pprint.pprint(result)
