import os
import pickle
import numpy as np
import pandas as pd
import requests
from ultralytics.nn.tasks import attempt_load_one_weight
import torch
import cv2
from torchvision import transforms
from ultralytics import YOLO



class YOLOFeatureExtractor():
    def __init__(self, model:str) -> None:
        # Tải mô hình YOLO-NAS (hoặc YOLO khác)
        self.__model = YOLO(model)
        # Access the backbone layers
        self.__backbone = self.__model.model.model[:10]  # Layers 0 to 9 form the backbone
        # Create a new Sequential model with just the backbone layers
        self.__backbone_model = torch.nn.Sequential(*self.__backbone)

    def cv2_read_image(self, image_bytes):
        image_array = np.asarray(bytearray(image_bytes), dtype=np.uint8)
        # Đọc ảnh bằng OpenCV
        image_rgb = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        # Chuyển đổi từ BGR (OpenCV) sang RGB
        # image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        return image_rgb

    def preprocess_image(self, image):
        preprocess = transforms.Compose([
            transforms.ToPILImage(),  # Chuyển từ numpy sang PIL
            transforms.Resize((640, 640)),  # Thay đổi kích thước ảnh về 640x640
            transforms.ToTensor(),  # Chuyển thành tensor
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])  # Chuẩn hóa theo chuẩn của ImageNet
        ])
        tensor_image = preprocess(image).unsqueeze(0)  # Thêm batch dimension
        return tensor_image

    def extract_features(self, image):
        with torch.no_grad():
            features = self.__backbone_model(image)
        return features

    def save_feature(self, features, WORKING_DIR, file_name):
        # Lưu features vào file pickle
        file_path = os.path.join(WORKING_DIR, file_name)
        # Lưu dữ liệu vào tệp
        with open(file_path, 'wb') as f:
            pickle.dump(features, f)

    def load_feature(self, WORKING_DIR, file_name):
        features = None
        # Đọc features từ file pickle
        if os.path.exists(f"{WORKING_DIR}/{file_name}"):
            with open(os.path.join(WORKING_DIR, file_name), 'rb') as f:
                features = pickle.load(f)
        return features
