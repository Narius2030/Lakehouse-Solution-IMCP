## Overal Architecture
![image](https://github.com/user-attachments/assets/e7fc0152-fe7c-4c00-8d83-c268d4fee4a9)


## Detailed Architecture
![image](https://github.com/user-attachments/assets/13726b7e-6c91-4453-a291-1dda31684cd1)

## Storage Structure in Data Lake:
![image](https://github.com/user-attachments/assets/5b9185f7-71c0-467a-a826-36881d5db6b6)



## Overal Data Pipeline
![image](https://github.com/user-attachments/assets/0f0e0040-8681-4b8f-9ba0-ec1eea828972)


## Practical Data Pipeline
At the `Bronze` layer:
* It will be divided into **3 DAGs** serving to collect data from sources
* Each DAG is responsible for collecting raw data from Parquet and user files (including images and metadata) from the source into MongoDB and MinIO aggregate stores

![image](https://github.com/user-attachments/assets/1bb6786b-38b4-4207-be2c-394e9d7dc9a7)

![image](https://github.com/user-attachments/assets/b4d65fbb-fd18-4ab7-8102-de535c38a960)

![image](https://github.com/user-attachments/assets/14e114a5-7ef7-47e8-b518-fae740a9f08d)

At the `Silver` and `Gold` layers:
* Silver layer is used to refine raw metadata from Bronze which will establish the refined metadata for `Catalog` layer in Data Lake
* Gold layer obtain to extract image feature from sources and save them in MinIO

![image](https://github.com/user-attachments/assets/85e16c66-f599-4191-9a9e-5ac05edb54b9)








