# General Architecture

- Builded a Data Lake following Medallion architecture with `catalog layer` and `storage layer` for storing image and its metadata
- Streamed events from `file uploading` and `captured images` from mobile app (was sent by API) into raw storage area, so that it helps data more various for AI training
- Integrated NLP and Image processings in ETL pipeline to periodically normalize images and metadata

![image](https://github.com/user-attachments/assets/923a659b-0401-4c68-a28b-704d6db14098)


## Detailed Architecture

![image](https://github.com/user-attachments/assets/64b1f8b2-22ce-4cdd-ac63-c8855883fbe0)

## Storage Structure

![image](https://github.com/user-attachments/assets/89c2aa4f-47a4-415e-a252-19f46bd7f3ef)

## MLOps Cycle

![image](https://github.com/user-attachments/assets/8c400e4c-48c5-4352-aa71-e2a4990cea85)

# FastAPI-based Microservice

- Develop an APIs to retrieve metadata and images which were normalized in Data Lake for automated incremental learning process.
- Develop an APIs to upload captured image and metadata of user to storage system for later usages and then activate model.
- Utilize Nginx to route and load balance among API service containers for **_reducing the latency_** and **_avoiding overload_** on each service.

![image](https://github.com/user-attachments/assets/11163700-dade-444e-8b19-d97bb7083237)


