# YouTubeDataExtractionAndAnalysis
## **Overview**  
One-stop solution to extract & analyze information related to your YouTube channel, videos and comments. With a channel ID, this tool will extract data at channel, video & comment level and gives you answers to some common queries basis the data extracted. The data is first stored in semi-structured format in MongoDB, then transformed & loaded into MySQL for easy analysis.  
## **Approach**  
**1. Set up a Streamlit app:** Streamlit is a great choice for building data visualization and analysis tools quickly and easily. We use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.  
**2. Connect to the YouTube API:** Google API client library for Python is used to make requests to the YouTube API.  
**3. Store data in a MongoDB data lake:** Once the data is retrieved from the YouTube API, it is stored in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.  
**4. Migrate data to a SQL data warehouse:** After data is collected for multiple channels, it is pushed to a MySQL data warehouse. 
**5. Query the SQL data warehouse:** SQL queries are run to retrieve data for the channels based on user input. We use the connector 'mysql.connector' to establish a connection with the SQL database.  
**6. Display data in the Streamlit app:** Finally, the retrieved data is displayed in the Streamlit app. 
## **Further Analysis**  
**1. Visualizations:** Streamlit's data visualization features to create charts and graphs to help users analyze the data  
**2. Sentiment Analysis on Comments:**  
**3. NLP Techniques to identify comment themes in comments (Cluster Analysis)**  
**4. Word Clouds on Comments:**  
