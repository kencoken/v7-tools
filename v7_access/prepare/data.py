import os
from typing import List

import boto3
from botocore.exceptions import ClientError
import concurrent.futures
import imagehash
from PIL import Image


class LocalData:

    def __init__(self, s3_src, local_src,
                 s3_bucket='tractable-eu-external-tooling-images'):
        #input paths
        self.s3_src = s3_src
        self.s3_bucket = s3_bucket
        # output paths
        self.local_src = local_src

    def download_images(self, image_ids: List[str], concurrent=False) -> List[str]:
        s3 = boto3.client('s3')

        def download_from_s3(image_id):
            if os.path.exists(f'tmnf/all/{image_id}.jpg'):
                return
            s3.download_file(self.s3_bucket, f'{self.s3_src}/{image_id}.jpg',
                             os.path.join(self.local_src, f'{image_id}.jpg'))

        if concurrent:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(download_from_s3, image_ids)
        else:
            skipped_image_ids = []

            for idx, image_id in enumerate(image_ids):
                if idx % 500 == 0:
                    print('{:,}/{:,} ({:,} skipped - {:.1f} %)'.format(idx, len(image_ids), len(skipped_image_ids),
                                                                       len(skipped_image_ids) / (idx + 1) * 100.0))
                try:
                    s3.download_file(self.s3_bucket, f'{self.s3_src}/{image_id}.jpg',
                                     os.path.join(self.local_src, f'{image_id}.jpg'))
                except ClientError as e:
                    skipped_image_ids.append(image_id)
        image_ids_local = [os.path.splitext(x)[0] for x in os.listdir(self.local_src)]

        return image_ids_local


    def find_non_duplicates(self, image_ids: List[str]) -> List[str]:
        """ Assumes images have been downloaded """
        image_hashes = {}
        image_hashes_rev = {}
        duplicates = []

        for image_id in image_ids:
            hash = imagehash.average_hash(Image.open(os.path.join(self.local_src, f'{image_id}.jpg')))

            if hash not in image_hashes:
                image_hashes[hash] = image_id
                image_hashes_rev[image_id] = hash
            else:
                if image_id not in image_hashes_rev:
                    duplicates.append(image_id)

        print(len(image_hashes))
        print(len(image_hashes_rev))
        print(len(duplicates))
        print(len(duplicates) / (len(image_hashes) + len(duplicates)) * 100.0)

        return list(image_hashes_rev.keys())
