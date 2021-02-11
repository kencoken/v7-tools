import os
import time
from zipfile import ZipFile
from typing import List

import requests
import numpy as np

from .data import VideoUpload
from ..utils import mktmpdir

from darwin.client import Client
from darwin.dataset.identifier import DatasetIdentifier

TEAM_SLUG = 'tractableteama'
API_KEY = '<api_key_here>'
HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'ApiKey {API_KEY}'
}


def create_dataset(dataset_slug):
    client = Client.from_api_key(API_KEY)
    identifier = DatasetIdentifier.parse(dataset_slug)
    dataset = client.create_dataset(name=identifier.dataset_slug)

    dataset_ifo = dict(
        name=dataset.name,
        id=dataset.dataset_id,
        slug=dataset.slug,
        remote_path=dataset.remote_path
    )
    return dataset_ifo

def add_labels_to_dataset(dataset_slug, labels: List[str], type: str):
    assert type in ['polygon', 'tag']
    client = Client.from_api_key(API_KEY)
    identifier = DatasetIdentifier.parse(dataset_slug)
    dataset = client.get_remote_dataset(dataset_identifier=identifier)

    for label in labels:
        dataset.create_annotation_class(label, 'polygon')

def populate_dataset_videos(dataset_id, videos: List[VideoUpload]):
    items = [dict(
        type='video',
        key=video.s3_vid_path,
        filename=video.filename,
        fps=1.0,
        frames=[dict(
            hq_key=s3_impath.path,
            lq_key=s3_impath.path,
            thumbnail=s3_impath.thumb_path,
            width=video.width,
            height=video.height
        ) for s3_impath in video.s3_image_paths]
    ) for video in videos]
    _populate_dataset(dataset_id, items)

def _populate_dataset(dataset_id, items):
    item_batches = [x.tolist() for x in np.array_split(items, 100)]
    for idx, batch in enumerate(item_batches):
        print(f'Batch {idx + 1}/{len(item_batches)}')
        payload = {
            'files': batch
        }
        response = requests.put(f'https://darwin.v7labs.com/api/datasets/{dataset_id}/external_data', headers=HEADERS,
                                json=payload)

        response.raise_for_status()

def get_annotations(dataset_slug, anno_dest_dir='annos'):
    client = Client.from_api_key(API_KEY)
    identifier = DatasetIdentifier.parse(dataset_slug)
    dataset = client.get_remote_dataset(dataset_identifier=identifier)

    filters = {'statuses': 'review,complete'}
    ids = [file.id for file in dataset.fetch_remote_files(filters)]

    # darwin-py doesn't support dataset_item_ids
    # uses also /datasets/{self.dataset_id}/exports
    # dataset.export(annotation_class_ids=annotation_class_ids, name=name, include_url_token=include_url_token)

    export_name = 'export_tmp'

    payload = dict(
        format='json',
        name=export_name,
        include_authorship=True,
        include_export_token=True,
        dataset_item_ids=ids
    )

    response_create = requests.post(f'https://darwin.v7labs.com/api/teams/{TEAM_SLUG}/datasets/{dataset_slug}/exports',
                                    headers=HEADERS,
                                    json=payload)
    response_create.raise_for_status()
    print('Creating export...')

    def get_export(timeout=60):
        waiting_for_export = True
        timeout_stop = time.time() + timeout
        while waiting_for_export:
            response_retrieve = requests.get(f'https://darwin.v7labs.com/api/teams/{TEAM_SLUG}/datasets/{dataset_slug}/exports', headers=HEADERS)
            response_retrieve.raise_for_status()
            exports = list(filter(lambda x: x['name'] == export_name, response_retrieve.json()))
            if len(exports) == 1 and exports[0]['latest']:
                return exports[0]
            else:
                if time.time() > timeout_stop:
                    raise RuntimeError('Timeout whilst waiting for export to complete')
    print('Waiting for export to complete...')
    export = get_export()

    # download export data
    # (this is also available through dataset.annotations as a single dict? maybe deprecated?)
    try:
        print('Downloading annotations...')
        with requests.get(export['download_url'], stream=True) as r:
            r.raise_for_status()
            with mktmpdir() as tmp_dir:
                tmp_file = os.path.join(tmp_dir, 'export.zip')
                with open(tmp_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                if os.path.exists(anno_dest_dir):
                    if len(os.listdir(anno_dest_dir)) > 0:
                        raise RuntimeError('Directory already exists and contains files!')
                else:
                    os.makedirs(anno_dest_dir)
                with ZipFile(tmp_file, 'r') as f:
                    f.extractall(anno_dest_dir)
    except Exception as e:
        response_delete = requests.delete(f'https://darwin.v7labs.com/api/teams/{TEAM_SLUG}/datasets/{dataset_slug}/exports/{export_name}', headers=HEADERS)
        response_delete.raise_for_status()
        raise e

    print('Export completed, cleaning up...')
    response_delete = requests.delete(f'https://darwin.v7labs.com/api/teams/{TEAM_SLUG}/datasets/{dataset_slug}/exports/{export_name}', headers=HEADERS)
    response_delete.raise_for_status()

    del export['download_url']
    return export
