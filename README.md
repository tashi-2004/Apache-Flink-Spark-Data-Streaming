# Data-Streaming-Flink-Spark
This repository demonstrates a real-time data streaming and persistence workflow using **Apache Flink**, **Apache Spark**, and **Grafana** for monitoring. The pipeline streams data, persists it in Parquet format, and performs aggregations for analytical insights

---

## Prerequisites

Ensure the following software is installed on your system:

- **Apache Flink**
- **Apache Spark**
- **Python 3.x**
- **Grafana**

For installation guidance, refer to tutorials available on YouTube or other online resources.

---

## Setup and Execution

Follow these steps to set up and execute the data streaming pipeline:

### 1. Start the Flink Cluster

1. Navigate to the `bin` folder in Flink's directory.
2. Run the command:
   ```bash
   ./start-cluster.sh
   ```
   ![Confirmation_JPS](https://github.com/user-attachments/assets/7c9af80c-fd89-491f-aa1a-ee875113f6b4)

### 2. Stream Data with Flink

1. Execute the following command to start streaming:
   ```bash
   python3 flink_streaming.py
   ```

2. The streaming process will begin, and you can monitor the progress in the terminal.
![Flink_Stream](https://github.com/user-attachments/assets/58c90e07-b2b3-4c8e-a7f3-cecd1c9fcac1)

### 3. Persist Data with Spark

1. Run the following command to process and persist data:
   ```bash
   python3 spark_persist.py
   ```
   ![spark_persist](https://github.com/user-attachments/assets/9bf996d4-6306-4df2-b39c-9032c570a1ad)
2. This step will create **seven Parquet files** in a folder named `spark_persisted_output` located in your home directory.
   
   ![persisted_output](https://github.com/user-attachments/assets/6c768f24-c923-48f4-9d2e-5326b772c56a)
### 4. Perform Aggregations

1. Execute the following command to perform data aggregation:
   ```bash
   python3 streaming_aggregates.py
   ```
   ![Daily_TotalRevenue](https://github.com/user-attachments/assets/9ca634ea-39ef-42e8-8820-79361a101afb)
2. The output will be stored in a folder named `aggregated_output` in your home directory.
   ![Daily_TotalRevenue_OutputFile](https://github.com/user-attachments/assets/eda3d112-e19d-4881-8838-984ce96e609b)

### 5. Set Up Grafana Dashboard

1. Download and install **Grafana**.
2. Visit Grafana at `http://localhost:3000` (default port).
3. Create a dashboard and import the provided JSON file to visualize the streaming and aggregated data.
   <img width="1265" alt="222" src="https://github.com/user-attachments/assets/2cef8d27-4b7c-4d7f-82d9-d9990f60d7f5" />
   <img width="1265" alt="1" src="https://github.com/user-attachments/assets/7ff18f4e-902a-4cd5-9566-0735c3bae975" />
---

## Note

- Update the paths for each Python file (`flink_streaming.py`, `spark_persist.py`, `streaming_aggregates.py`) according to your system setup.
- Ensure all required dependencies are installed and configured correctly.
- You can download the original dataset from: [Download](https://mega.nz/file/OJUxVKCB#vWVfFYmnAzAM0PTMBZRSmmrWePcmoN1qIpM0kd4zFRw)

---

## Repository Structure

```
Data-Streaming-Flink-Spark/
├── flink_streaming.py        # Flink streaming script
├── spark_persist.py          # Spark persistence script
├── streaming_aggregates.py   # Data aggregation script
├── dashboard.json    # Grafana dashboard configuration file
├── spark_persisted_output/   # Output folder for Spark persisted data
├── aggregated_output/        # Output folder for aggregated data
```

---

## Contact

For queries or contributions, please contact:
**Tashfeen Abbasi**  
Email: abbasitashfeen7@gmail.com
