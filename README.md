# General Architecture of Data Lake

- Builded a Data Lake following Medallion architecture with `catalog layer` and `storage layer` for storing image and its metadata
- Streamed events from `file uploading` and `captured images` from mobile app (was sent by API) into raw storage area, so that it helps data more various for AI training
- Integrated NLP and Image processings in ETL pipeline to periodically normalize images and metadata

![image](https://github.com/user-attachments/assets/923a659b-0401-4c68-a28b-704d6db14098)

Real-time Dashboard for ingested data

![eda](https://github.com/user-attachments/assets/e6d79c8e-3637-482f-b988-b6afe52f3627)


## Detailed Architecture

![image](https://github.com/user-attachments/assets/64b1f8b2-22ce-4cdd-ac63-c8855883fbe0)

## MLOps Cycle

![image](https://github.com/user-attachments/assets/8c400e4c-48c5-4352-aa71-e2a4990cea85)

# FastAPI-based Microservice

> More detail in this [Repo](https://github.com/Narius2030/FastAPI-Microservice-IMCP.git)

- Develop an APIs to retrieve metadata and images which were normalized in Data Lake for automated incremental learning process.
- Develop an APIs to upload captured image and metadata of user to storage system for later usages and then activate model.
- Utilize Nginx to route and load balance among API service containers for **_reducing the latency_** and **_avoiding overload_** on each service.

![image](https://github.com/user-attachments/assets/11163700-dade-444e-8b19-d97bb7083237)


