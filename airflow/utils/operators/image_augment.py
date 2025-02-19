import pandas as pd
import os
import io
import cv2
import time
import numpy as np
import requests
from PIL import Image
import albumentations as A
from tqdm import tqdm
from utils.operators.storage import MinioStorageOperator

# Định nghĩa các đường dẫn
INPUT_CSV = "data_v2/label_captions_cleaned_filtered.csv"
OUTPUT_DIR = "data_v2/augmented"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "captions_augmented.csv")


# df = pl.DataFrame(data)
# affected_rows += len(data)


# Tạo thư mục output nếu chưa tồn tại
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "images"), exist_ok=True)

# Định nghĩa các augmentation transforms
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

def upload_image(image_matrix, image_name, bucket_name, file_path, settings):
    minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                        access_key=settings.MINIO_USER,
                                        secret_key=settings.MINIO_PASSWD)

    image_bgr = cv2.cvtColor(image_matrix, cv2.COLOR_RGB2BGR)
    
    _, encoded_image = cv2.imencode('.jpg', image_bgr)
    image_bytes = io.BytesIO(encoded_image)
    minio_operator.upload_object_bytes(image_bytes, bucket_name, f'{file_path}/{image_name}', "image/jpeg")

def cv2_read_image(image_bytes):
    # image_array = np.asarray(bytearray(image_bytes), dtype=np.uint8)
    # if image_array is None or image_array.size == 0:
    #     raise ValueError("image_array is empty. Check if the image was loaded correctly.")
    # image_rgb = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
    # Chuyển đổi từ BGR (OpenCV) sang RGB
    # if image_rgb.mode not in ['RGB', 'L']:
    #     image_rgb = cv2.cvtColor(image_rgb, cv2.COLOR_BGR2RGB)
    
    # Đọc ảnh bằng PIL
    image = Image.open(io.BytesIO(image_bytes))
    # Chuyển đổi ảnh sang RGB nếu cần
    if image.mode not in ['RGB', 'L']:
        image = image.convert('RGB')
    
    # Chuyển sang numpy array
    image_rgb = np.array(image)
    
    # Đảm bảo ảnh có đúng số kênh màu (1 hoặc 3)
    if len(image_rgb.shape) == 2:  # Ảnh grayscale
        image_rgb = np.stack([image_rgb] * 3, axis=-1)  # Chuyển thành 3 kênh
    elif len(image_rgb.shape) == 3 and image_rgb.shape[2] == 4:  # Ảnh RGBA
        image_rgb = image_rgb[:, :, :3]  # Bỏ kênh alpha
    
    return image_rgb

def image_from_url(image_url:str):
    try:
        image_repsonse = requests.get(image_url, timeout=2)
        image_rgb = cv2_read_image(image_repsonse.content)
        return image_rgb, False
    except Exception:
        for attempt in range(0, 2):
            try:
                image_repsonse = requests.get(image_url, timeout=2)
                image_rgb = cv2_read_image(image_repsonse.content)
                return image_rgb, False  # Thành công, thoát khỏi vòng lặp thử lại
            except Exception as e:
                print(f"Tải lại dữ liệu từ {image_url} (lần {attempt+1}/{2}): {str(e)}")
                time.sleep(2)  # Chờ đợi trước khi thử lại
    return None, True
    

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
            augmented = transforms(image=image)['image']
            augmented_images.append(augmented)
            
        return augmented_images
    except Exception as e:
        print(f"Lỗi chi tiết khi xử lý ảnh")
        print(f"Loại lỗi: {type(e).__name__}")
        print(f"Nội dung lỗi: {str(e)}")
        return []

def main():
    # Đọc file CSV gốc
    df = pd.read_csv(INPUT_CSV)
    print(f"Đọc được {len(df)} ảnh từ file CSV")
    
    # List để lưu dữ liệu mới
    new_data = []
    
    # Xử lý từng ảnh
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Augmenting images"):
        original_path = row['local_path']
        if not os.path.exists(original_path):
            print(f"Không tìm thấy ảnh: {original_path}")
            continue
            
        # Copy ảnh gốc vào thư mục mới
        original_filename = os.path.basename(original_path)
        new_original_path = os.path.join(OUTPUT_DIR, "images", original_filename)
        try:
            img = Image.open(original_path)
            img.save(new_original_path)
            
            # Thêm ảnh gốc vào danh sách với đường dẫn mới
            new_data.append({
                'original_url': row['original_url'],
                'source_website': row['source_website'],
                'resolution': row['resolution'],
                'search_query': row['search_query'],
                'local_path': new_original_path,
                'caption': row['caption']
            })
        except Exception as e:
            print(f"Lỗi khi copy ảnh gốc {original_filename}: {str(e)}")
            continue
            
        # Tạo các phiên bản augmented
        augmented_images = augment_image(original_path)
        
        # Lưu các ảnh augmented và thêm vào DataFrame
        for aug_idx, aug_image in enumerate(augmented_images):
            try:
                # Tạo tên file mới
                name, ext = os.path.splitext(original_filename)
                new_name = f"{name}_aug_{aug_idx}{ext}"
                new_path = os.path.join(OUTPUT_DIR, "images", new_name)
                
                # Lưu ảnh
                pil_image = Image.fromarray(aug_image)
                if pil_image.mode != 'RGB':
                    pil_image = pil_image.convert('RGB')
                pil_image.save(new_path)
                
                # Thêm vào danh sách dữ liệu mới với caption giống ảnh gốc
                new_data.append({
                    'original_url': row['original_url'],
                    'source_website': row['source_website'],
                    'resolution': row['resolution'],
                    'search_query': row['search_query'],
                    'local_path': new_path,
                    'caption': row['caption']  # Giữ nguyên caption từ ảnh gốc
                })
            except Exception as e:
                print(f"Lỗi khi lưu ảnh augmented {new_name}: {str(e)}")
                continue
    
    # Tạo DataFrame mới và lưu
    new_df = pd.DataFrame(new_data)
    new_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Đã lưu file CSV mới tại: {OUTPUT_CSV}")
    print(f"Tổng số ảnh (gốc + augmented): {len(new_df)}")




if __name__ == "__main__":
    main()