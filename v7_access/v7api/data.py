from typing import NamedTuple, List

import numpy as np
import pandas as pd
import imageio
from PIL import Image, ImageOps


class S3Path(NamedTuple):
    path: str
    thumb_path: str


class ImageUpload(NamedTuple):
    filename: str
    s3_path: str


class VideoUpload(NamedTuple):
    filename: str
    s3_vid_path: str
    s3_image_paths: List[S3Path]
    width: int
    height: int


class VideoUploader:

    def __init__(self, local_src: str, local_root: str, s3_root: str,
                 s3_bucket='tractable-eu-external-tooling-images'):
        # input paths
        self.local_src = local_src  # local location of images
        # output paths
        self.local_root = local_root  # local root to populate with experiment: {local_root}/{experiment_name}
        self.s3_root = s3_root  # remote root to upload to: {s3_root}/{experiment_name}
        self.s3_bucket = s3_bucket  # s3 bucket to upload to

    def prepare_upload(self, df: pd.DataFrame, experiment_name: str) -> List[VideoUpload]:
        """ Create experiment data locally for video upload, upload to s3, return details
        input: df contains imbag_id and image_id columns -> look for images of form {image_id}.jpg
        output: list of video upload metadata instances (for passing to dataset.populate_dataset_videos)
        """
        uploads = []

        for imbag_id, df_group in df.groupby('imbag_id'):
            image_ids = df_group['image_id'].tolist()
            input_images = [f'{self.local_src}/{image_id}.jpg' for image_id in image_ids]
            output_images = [f'{self.local_root}/{experiment_name}/{imbag_id}_{image_id}.jpg' for image_id in image_ids]
            output_avi_path = f'{self.local_root}/{experiment_name}/{imbag_id}.avi'
            output_thumb_paths = [f'{self.local_root}/{experiment_name}/_thumbs/{imbag_id}_{image_id}.jpg'
                                  for image_id in image_ids]

            s3_output_images = [f'{self.s3_root}/{experiment_name}/{imbag_id}_{image_id}.jpg' for image_id in image_ids]
            s3_output_avi_path = f'{self.s3_root}/{experiment_name}/{imbag_id}.avi'
            s3_output_thumb_paths = [f'_thumbs/{self.s3_root}/{experiment_name}/{imbag_id}_{image_id}.jpg'
                                     for image_id in image_ids]

            # save avi file
            ims = [imageio.imread(input_image) for input_image in input_images]
            shapes = [im.shape for im in ims]
            max_dim = None
            if not all(x == shapes[0] for x in shapes):
                max_dim = max(max(x[0] for x in shapes), max(x[1] for x in shapes))
                ims = [np.array(ImageOps.pad(Image.fromarray(im), [max_dim, max_dim])) for im in ims]
            imageio.mimwrite(output_avi_path, ims, 'FFMPEG', fps=1, macro_block_size=1, codec='mjpeg', quality=10)

            # save images
            w, h, _ = shapes[0] if max_dim is None else [max_dim, max_dim, 0]
            for im, output_path, output_thumb_path in zip(ims, output_images, output_thumb_paths):
                imageio.imwrite(output_path, im, quality=100)
                # create thumb also
                pil_im = Image.fromarray(im)
                sf = min(1.0, 356 / max(w, h))
                pil_im.thumbnail([w * sf, h * sf])
                pil_im.save(output_thumb_path, quality=50)

            # prepare upload object
            uploads.append(VideoUpload(
                filename=imbag_id,
                s3_vid_path=s3_output_avi_path,
                s3_image_paths=[S3Path(path=path, thumb_path=thumb_path)
                                for path, thumb_path in zip(s3_output_images, s3_output_thumb_paths)],
                width=w,
                height=h
            ))

        # upload to s3
        print('Run the following to upload processed data to S3:')
        print(f'aws s3 cp {self.local_root}/{experiment_name} s3://{self.s3_bucket}/{self.s3_root}/{experiment_name}/ --recursive --exclude "*thumbs*"')
        print(f'aws s3 cp {self.local_root}/{experiment_name}/_thumbs s3://{self.s3_bucket}/{self.s3_root}/{experiment_name}/ --recursive')

        return uploads
