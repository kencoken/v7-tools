import os
import json
import pprint

from v7_access.utils import mktmpdir
from v7_access.v7api import dataset
from v7_access.v7api.data import ImageUpload

dataset_slug = 'anno_test'

result = dataset.create_dataset(dataset_slug)
pprint.pprint(result)
dataset.add_labels_to_dataset(dataset_slug, ['snippet'], 'polygon')

filename = 'test'
s3_path = 'tmnf/202010_minor_damage_grouped_ims/00023779-2b2c-39e8-9553-655c4b8e1e30_393b0c46-b37e-5869-8d37-04351d737cc7.jpg'
dataset.populate_dataset_images(dataset_slug, [ImageUpload(filename, s3_path)])

with mktmpdir() as tmpdir:
    anno = dict(
        image=dict(
            filename=filename,
            width=360,
            height=480,
            url=''
        ),
        annotations=[dict(
            name='snippet',
            polygon=dict(
                path=[
                    {'x': 10, 'y': 10},
                    {'x': 90, 'y': 10},
                    {'x': 80, 'y': 210},
                    {'x': 5.7, 'y': 20}
                ]
            )
        )]
    )
    tmp_anno_path = os.path.join(tmpdir, 'test.json')
    with open(tmp_anno_path, 'w') as f:
        json.dump(anno, f)
    dataset.populate_dataset_annotations(dataset_slug, 'darwin', [tmp_anno_path])

pprint.pprint(list(dataset.get_dataset_files(dataset_slug)))
