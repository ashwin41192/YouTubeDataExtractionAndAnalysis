#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Import Packages
import streamlit as st
import pymongo
import mysql.connector as sql
import pandas as pd
from PIL import Image
from streamlit_option_menu import option_menu
from bson import ObjectId
from googleapiclient.discovery import build


# In[3]:


#Setting up Landing Page for Streamlit
icon = Image.open("C:/Users/ashwi/OneDrive/Desktop/Youtube Exercise/acastro_STK092_02.jpg")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | Ashwin Satish",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app can be used to extract and analyze YouTube Data!*"""})

#Creating Tabs for each phase of process
#For Icons visit https://icons.getbootstrap.com/
#For Info on st.sidebar option visit https://docs.streamlit.io/library/api-reference/layout/st.sidebar

with st.sidebar:
    selected = option_menu(None, ["Home","Extract Data","Analyze"], 
                           icons=["house-fill","person-fill-gear","question-square-fill"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#cfa8d6"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#cfa8d6"}})


# In[4]:


#Connecting to MongoDB
myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
mydb = myclient["youtube_data"]


# In[27]:


#Connecting to MySQL
mydbsql = sql.connect(host="localhost",
                   user="root",
                   password="Msai16yo#",
                   database="youtube_data",
                   auth_plugin='mysql_native_password',
                   charset="utf8mb4"
                  )
mycursor = mydbsql.cursor(buffered=True)


# In[7]:


#Defining a function that extracts all channel level info into a data dictionary with an API using using Channel ID

def get_channel_data(channel_id_no):
    api_key = 'AIzaSyBp99zS6YrOL95ATBwh5WSLAzToo59QABI'
    youtube = build('youtube','v3',developerKey = api_key)
    
    #For API details visit: https://developers.google.com/youtube/v3/docs/channels/list?apix=true
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id_no
        )
    response = request.execute()

    channel_data = []
    for i in range(len(response['items'])):
        data = dict(channel_id = response['items'][i]['id'],
                    channel_name = response['items'][i]['snippet']['title'],
                    channel_views = int(response['items'][i]['statistics']['viewCount']),
                    channel_video_count = int(response['items'][i]['statistics']['videoCount']),
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    channel_subscribers = int(response['items'][i]['statistics']['subscriberCount']),
                    channel_description = response['items'][i]['snippet']['description'])
        channel_data.append(data)
    return channel_data


# In[8]:


#Defining a function that extracts all playlist info into a data dictionary by using Channel ID as input

def get_channel_videos(channel_id):
    video_ids = []
    api_key = 'AIzaSyBp99zS6YrOL95ATBwh5WSLAzToo59QABI'
    youtube = build('youtube','v3',developerKey = api_key)
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


# In[9]:


#Defining a function that extracts all video info into a data dictionary by using Channel ID as input

def get_video_details(v_ids):
    video_stats = []
    api_key = 'AIzaSyBp99zS6YrOL95ATBwh5WSLAzToo59QABI'
    youtube = build('youtube','v3',developerKey = api_key)
    for i in range(0, len(v_ids), 50): #Max of 50 videos will be extracted
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# In[10]:


#Defining a function that extracts all comment info into a data dictionary by using Video ID as input

def get_comments_details(v_id):
    comment_data = []
    api_key = 'AIzaSyBp99zS6YrOL95ATBwh5WSLAzToo59QABI'
    youtube = build('youtube','v3',developerKey = api_key)
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100, #Max 100 comments
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# In[111]:


#Building Functionality for each tab availabe in app

if selected == "Home":
    st.image(icon)
    st.write("""
        # Welcome to my YouTube Data Extractor!
        One-stop solution to extract & analyze information related to your YouTube channel, videos and comments.
        With a channel ID, this tool will extract data at channel, video & comment level and gives you answers to some common queries basis the data extracted.
        The data is first stored in semi-structured format in MongoDB, then transformed & loaded into MySQL for easy analysis.
        """)
    st.write("""
        To update database with new channel data click on "Extract Data" tab. To analyze extracted data click on "Analyze"
        """)
    mycursor.execute("""SELECT distinct channel_name AS Channels_Currently_Included_In_DB
                        FROM channel
                        ORDER BY channel_name""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    
if selected == "Extract Data":
    st.write("""
        # Extract Data
        """)
    #Getting user input for channel ID    
    ch_id = st.text_input("Please Enter Channel ID for the YouTube Channel You Wish to Analyze").split(',')
    if ch_id and st.button("Step 1: Check Channel ID"):
        ch_details = get_channel_data(ch_id)
        st.write(f'#### Channel Name :purple["{ch_details[0]["channel_name"]}"] channel')
        st.table(ch_details)

    if st.button("Step 2: Upload to MongoDB"):
        with st.spinner('Processing...'):
            ch_details = get_channel_data(ch_id)
            v_ids = get_channel_videos(ch_id)
            vid_details = get_video_details(v_ids)
                
            def comments():
                com_d = []
                for i in v_ids:
                    com_d+= get_comments_details(i)
                return com_d
            comm_details = comments()

            collections1 = mydb.channel_details
            collections1.insert_many(ch_details)

            collections2 = mydb.video_details
            collections2.insert_many(vid_details)

            collections3 = mydb.comments_details
            collections3.insert_many(comm_details)
            st.success("Data Migration to MongoDB successful !!")

    if st.button("Step 3: Push to MySQL"):
        #Defining function to move channels info into channel table in MySQL
        def insert_into_channels():
            collections = mydb.channel_details
            query = """INSERT INTO channel VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""        
            for document in collections.find():
                document_values = [str(value) if isinstance(value, ObjectId) else value for value in document.values()]
                mycursor.execute(query,tuple(document_values))
                mydbsql.commit()
        
        #Defining function to move videos info into video_5 table in MySQL
        def insert_into_videos():
            collections2 = mydb.video_details
            query = """INSERT INTO video_5 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""   
            for document in collections2.find():
                document_values = [
                str(value) if isinstance(value, ObjectId) else value
                for value in document.values()
                ]

                # Flatten the list before inserting into MySQL
                flattened_list = ",".join(document_values[5]) if isinstance(document_values[5], list) else document_values[5]
                document_values[5] = flattened_list

                mycursor.execute(query, tuple(document_values))
                mydbsql.commit()
        
        #Defining function to move comments info into comment_2 table in MySQL
        def insert_into_comments():
            collections3 = mydb.comments_details
            query = """INSERT INTO comment_2 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""        
            for document in collections3.find():
                document_values = [
                str(value) if isinstance(value, ObjectId) else value
                for value in document.values()
                ]
    
                mycursor.execute(query,tuple(document_values))
                mydbsql.commit()            
                
        try:
                
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                st.success("Transformation to MySQL Successful!!!")
                mydb['channel_details'].delete_many({})
                mydb['comments_details'].delete_many({})
                mydb['video_details'].delete_many({})
        except:
            st.error("Channel details already transformed!!")
            
if selected == "Analyze":    
    st.write("## :purple[Select any question to get Insights]")
    #Creating a dropdown with possible questions
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    #Defining SQL queries attached to each question available in drop down
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name 
                            FROM video_5
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_video_count AS Total_Videos
                            FROM channel
                            ORDER BY channel_video_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM video_5
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM video_5 AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment_2 GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM video_5
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM video_5
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM channel
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM video_5
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, 
                            SUM(duration_sec) / COUNT(*) AS average_duration
                            FROM (
                                    SELECT channel_name, 
                                    CASE WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                    TIME_TO_SEC(CONCAT(
                                                        SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                                                        SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                                                        SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                    WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                    TIME_TO_SEC(CONCAT(
                                                        '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                                        SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                    WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                    TIME_TO_SEC(CONCAT(
                                                        '0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                    END AS duration_sec
                                    FROM video_5) AS subquery
                            GROUP BY channel_name """)
        
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID, title AS Video_Title, comments AS Comments
                            FROM video_5
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

