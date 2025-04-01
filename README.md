# General Architecture of Data Lake

- Builded a Data Lake following Medallion architecture with `catalog layer` and `storage layer` for storing image and its metadata
- Streamed events from `file uploading` and `captured images` from mobile app (was sent by API) into raw storage area, so that it helps data more various for AI training
- Integrated NLP and Image processings in ETL pipeline to periodically normalize images and metadata

![image](https://github.com/user-attachments/assets/923a659b-0401-4c68-a28b-704d6db14098)

Real-time Dashboard for ingested data

![imcp-eda-2025-03-31T16-07-27 174Z](https://github.com/user-attachments/assets/7995702a-c71f-47fc-8b9f-1e38b88e4d90)


## Detailed Architecture

![image](https://github.com/user-attachments/assets/da4c583d-70fd-440d-b2cc-9ab08cf92fd2)

## MLOps Cycle

![image](https://github.com/user-attachments/assets/31effa68-39dc-4c92-860a-074c959b911a)

# FastAPI-based Microservice

> More detail in this [Repo](https://github.com/Narius2030/FastAPI-Microservice-IMCP.git)

- Develop an APIs to retrieve metadata and images which were normalized in Data Lake for automated incremental learning process.
- Develop an APIs to upload captured image and metadata of user to storage system for later usages and then activate model.
- Utilize Nginx to route and load balance among API service containers for **_reducing the latency_** and **_avoiding overload_** on each service.

![image](https://github.com/user-attachments/assets/11163700-dade-444e-8b19-d97bb7083237)


