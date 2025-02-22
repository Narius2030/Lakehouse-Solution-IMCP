import io
import cv2
import time
import requests
import numpy as np
import albumentations as A
from PIL import Image
from transformers import AutoTokenizer
from utils.operators.storage import MinioStorageOperator


class ImageOperator():
    transforms = A.Compose([
        # Biến đổi về cường độ pixel
        A.OneOf([
            A.RandomBrightnessContrast(
                brightness_limit=0.2,
                contrast_limit=0.2,
                p=0.5
            ),
            A.ColorJitter(
                brightness=0.2,
                contrast=0.2,
                saturation=0.2,
                hue=0.1,
                p=0.5
            ),
        ], p=0.5),
        
        # Biến đổi không gian
        A.OneOf([
            A.ShiftScaleRotate(
                shift_limit=0.1,
                scale_limit=0.1,
                rotate_limit=15,
                border_mode=cv2.BORDER_CONSTANT,
                p=0.5
            ),
            A.Affine(
                scale=(0.9, 1.1),
                translate_percent={"x": (-0.1, 0.1), "y": (-0.1, 0.1)},
                rotate=(-10, 10),
                p=0.5
            ),
        ], p=0.5),
        
        # Thêm nhiễu hoặc làm mờ nhẹ
        A.OneOf([
            A.GaussNoise(var_limit=(10.0, 30.0), p=0.5),
            A.GaussianBlur(blur_limit=(3, 5), p=0.5),
        ], p=0.3),
        
        # Random crop với tỷ lệ cao
        A.RandomResizedCrop(
            height=512,
            width=512,
            scale=(0.8, 1.0),
            ratio=(0.9, 1.1),
            p=0.5
        ),
        
        # Đảm bảo kích thước output đồng nhất
        A.Resize(512, 512, p=1.0)
    ])

    @staticmethod
    def upload_image(image_matrix, image_name, bucket_name, file_path, settings):
        minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                            access_key=settings.MINIO_USER,
                                            secret_key=settings.MINIO_PASSWD)

        image_bgr = cv2.cvtColor(image_matrix, cv2.COLOR_RGB2BGR)
        
        _, encoded_image = cv2.imencode('.jpg', image_bgr)
        image_bytes = io.BytesIO(encoded_image)
        minio_operator.upload_object_bytes(image_bytes, bucket_name, f'{file_path}/{image_name}', "image/jpeg")


    @staticmethod
    def cv2_read_image(image_bytes):
        # Đọc ảnh bằng PIL
        image = Image.open(io.BytesIO(image_bytes))
        # Chuyển đổi ảnh sang RGB nếu cần
        if image.mode not in ['RGB', 'L']:
            image = image.convert('RGB')
        image_rgb = np.array(image)
        
        # Đảm bảo ảnh có đúng số kênh màu (1 hoặc 3)
        if len(image_rgb.shape) == 2:  # Ảnh grayscale
            image_rgb = np.stack([image_rgb] * 3, axis=-1)  # Chuyển thành 3 kênh
        elif len(image_rgb.shape) == 3 and image_rgb.shape[2] == 4:  # Ảnh RGBA
            image_rgb = image_rgb[:, :, :3]  # Bỏ kênh alpha
        
        return image_rgb


    @staticmethod
    def image_from_url(image_url:str):
        try:
            image_repsonse = requests.get(image_url, timeout=2)
            image_rgb = ImageOperator.cv2_read_image(image_repsonse.content)
            return image_rgb, False
        except Exception:
            for attempt in range(0, 2):
                try:
                    image_repsonse = requests.get(image_url, timeout=2)
                    image_rgb = ImageOperator.cv2_read_image(image_repsonse.content)
                    return image_rgb, False  # Thành công, thoát khỏi vòng lặp thử lại
                except Exception as e:
                    print(f"Tải lại dữ liệu từ {image_url} (lần {attempt+1}/{2}): {str(e)}")
                    time.sleep(2)  # Chờ đợi trước khi thử lại
        return None, True


    @staticmethod
    def augment_image(image, num_augmentations=3):
        """Tạo nhiều phiên bản augmented của một ảnh"""
        try:
            # Chuyển đổi ảnh sang RGB nếu cần
            image = Image.fromarray(image)
            if image.mode not in ['RGB', 'L']:
                image = image.convert('RGB')
            # Đảm bảo ảnh có đúng số kênh màu (1 hoặc 3)
            image = np.array(image)
            if len(image.shape) == 2:  # Ảnh grayscale
                image = np.stack([image] * 3, axis=-1)  # Chuyển thành 3 kênh
            elif len(image.shape) == 3 and image.shape[2] == 4:  # Ảnh RGBA
                image = image[:, :, :3]  # Bỏ kênh alpha
            
            augmented_images = []
            for i in range(num_augmentations):
                augmented = ImageOperator.transforms(image=image)['image']
                augmented_images.append(augmented)
                
            return augmented_images
        except Exception as e:
            print(f"Lỗi chi tiết khi xử lý ảnh")
            print(f"Loại lỗi: {type(e).__name__}")
            print(f"Nội dung lỗi: {str(e)}")
            return []
        
    
    @staticmethod
    def encode_caption(caption:str):
        # Load tokenizer của BartPho
        tokenizer = AutoTokenizer.from_pretrained("vinai/bartpho-word")
        # encode caption
        tokenized = tokenizer.encode_plus(caption, return_tensors="pt", padding="max_length", truncation=True)
        tokenized_dict = dict(tokenized)
        return tokenized_dict