
import streamlit as st
import pymongo
import requests
import pandas as pd
import json
import mysql.connector
import sqlalchemy 
from sqlalchemy import create_engine
import pymysql
import re
from mysql.connector.plugins import caching_sha2_password

st.title(':red[Youtube data harvesting]')
tab1, tab2, tab3 = st.tabs(["Search and Migrate to Mongodb", "Migrate to Sql", "Questions"])


#youtube api request and migration to mongo db

with tab1:
  st.subheader('Enter channel Id :')
  channelid=st.text_input('')
  channel_name=[]
  total_data= []
  migrate = st.checkbox('Migrate to mongodb')
  if (st.button('search') and channelid != ''):
    response = requests.get("https://www.googleapis.com/youtube/v3/channels?key=AIzaSyBoXfl-_T5Su86wgVDjj3v1XPgXBbPvfuk&part=snippet,statistics,contentDetails&order=date&maxResults=20&id={channelid}".format(channelid
    =channelid))
    data=response.json()
    channel_name = data['items'][0]['snippet']['title']
    channel_video_count = data['items'][0]['statistics']['videoCount']
    channel_subscriber_count = data['items'][0]['statistics']['subscriberCount']
    channel_view_count = data['items'][0]['statistics']['viewCount']
    channel_description = data['items'][0]['snippet']['description']

    channel_playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
 
    #st.write(channel)
    video_id=[]
    h=0
    response1=requests.get("https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={channel_playlist_id}&key=AIzaSyBoXfl-_T5Su86wgVDjj3v1XPgXBbPvfuk".format(channel_playlist_id=channel_playlist_id))
    response2=response1.json()
    # st.write(response2)
    for item in response2['items']:
       video_id.append(item['contentDetails']['videoId'])
       h=h+1
    #st.write(video_id,h)
  
    video_data = {} 
    for item1 in range(h):
       commentvariable=0
       video_id1=video_id[item1]
       video_response1=requests.get("https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id={video_id1}&key=AIzaSyBoXfl-_T5Su86wgVDjj3v1XPgXBbPvfuk".format(video_id1=video_id1))
       video_response2=video_response1.json()
       #st.write(video_response2)
       video_id1=[]
       video_id_1=video_response2['items'][0]['id']
       video_name_1=video_response2['items'][0]['snippet']['title']
       video_description_1=video_response2['items'][0]['snippet']['description']
       video_tags_1=video_response2['items'][0]['etag']
       video_publishedat_1=video_response2['items'][0]['snippet']['publishedAt']
       video_view_count=video_response2['items'][0]['statistics']['viewCount']
       video_like_count=video_response2['items'][0]['statistics']['likeCount']
       #video_dislike_count=video_response2['items'][0]['statistics']['dislikeCount']
       video_favoriteCount=video_response2['items'][0]['statistics']['favoriteCount']
       video_comment_count_1=video_response2['items'][0]['statistics']['commentCount']
       duration1 = video_response2['items'][0]['contentDetails']['duration']
       thumbnail=video_response2['items'][0]['snippet']['thumbnails']['default']['url']
       caption=video_response2['items'][0]['contentDetails']['caption']
       
  
       def convert_duration(duration1):
          regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
          match = re.match(regex, duration1)
          if not match:
              return '00:00:00'
          hours, minutes, seconds = match.groups()
          hours = int(hours[:-1]) if hours else 0
          minutes = int(minutes[:-1]) if minutes else 0
          seconds = int(seconds[:-1]) if seconds else 0
          total_seconds = hours * 3600 + minutes * 60 + seconds
          return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))
 
       duration2 = convert_duration(duration1)     
       comment_response1=requests.get("https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id_1}&maxResults=10&key=AIzaSyBoXfl-_T5Su86wgVDjj3v1XPgXBbPvfuk".format(video_id_1=video_id_1))
       comment_response2=comment_response1.json()
       comment_id=[]
       commentvariable=1
       comment_data={}
       comment_data3={}
       for comment in comment_response2['items']:
          
          comment_id1=comment['id']
          comment_author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
          comment_text=comment['snippet']['topLevelComment']['snippet']['textOriginal']
          comment_publishedat=comment['snippet']['topLevelComment']['snippet']['publishedAt']
          comment_data3[f"Comment_Id_{commentvariable}"]={
                          'Comment_Id': comment_id1,
                          'Comment_Text': comment_text,
                          'Comment_Author': comment_author,
                          'Comment_PublishedAt': comment_publishedat
                        }
                      
          commentvariable=commentvariable+1
          #comment_data=json.dumps(comment_data2)
          #comment_data3=comment_data+comment_data
          #total_data.append(comment_data) 
          #st.write(comment_data)   
          #total_data=json.dumps(total_data)
    #total_data2={channel+total_data}

       video_data[f"video_Id_{item1}"] = {
              "video_id": video_id_1,
              "video_name": video_name_1,
              "video_description": video_description_1,
              "tags": video_tags_1,
              "publishedat": video_publishedat_1,
              "view_count": video_view_count,
              "like_count": video_like_count,
              #"dislike_count": video_dislike_count,
              "favorite_Count": video_favoriteCount,
              "comment_count": video_comment_count_1,
              "duration": duration2,
              "thumbnail": thumbnail,
              "caption_status": caption,
              "comments" : comment_data3
         
           
       }
        #st.write(video_data) 
      #video_data=json.dumps(video_data2)
    #total_data.append(video_data)
    channel = {
            "Channel_Details": {
                "Channel_Name": channel_name,
                "Channel_Id": channelid,
                "Video_Count": channel_video_count,
                "Subscriber_Count": channel_subscriber_count,
                "Channel_Views": channel_view_count,
                "Channel_Description": channel_description,
                "Playlist_Id": channel_playlist_id
    }
    }
    #channel=json.dumps(channel2)
    total_data.append(channel)
    total_data.append(video_data)
    st.write(total_data)
    if migrate:
      client = pymongo.MongoClient('mongodb://localhost:27017/')
      mydb = client['youtube']
      collection = mydb['youtubeapi']
      final_data = {
                'Channel_Name': channel_name,
                "Channel_data":total_data
                }
      upload = collection.replace_one({'_id': channelid}, final_data, upsert=True)
      st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")
      client.close()
      
    
  else:
    st.write('')
  
with tab2:
   st.header(':violet[Data Migrate to SQL]')
   client = pymongo.MongoClient('mongodb://localhost:27017/')
   mydb = client['youtube']
   collection = mydb['youtubeapi']
   document_names = []
   for document in collection.find():
       document_names.append(document["Channel_Name"])
   document_name = st.selectbox('**Select Channel name**', options = document_names, key='document_names')
   st.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
   Migrate = st.button('**Migrate to MySQL**')
    
     # Define Session state to Migrate to MySQL button
   if 'migrate_sql' not in st.session_state:
       st.session_state_migrate_sql = False
   if Migrate or st.session_state_migrate_sql:
       st.session_state_migrate_sql = True
       # Retrieve the document with the specified name
       result = collection.find_one({"Channel_Name": document_name})
       #st.write(result)
       client.close()
       # Channel data json to df
       channel_details_to_sql = {
           "Channel_Name": result['Channel_Name'],
           "Channel_Id": result['_id'],
           "Video_Count": result['Channel_data'][0]['Channel_Details']['Video_Count'],
           "Subscriber_Count": result['Channel_data'][0]['Channel_Details']['Subscriber_Count'],
           "Channel_Views": result['Channel_data'][0]['Channel_Details']['Channel_Views'],
           "Channel_Description": result['Channel_data'][0]['Channel_Details']['Channel_Description'],
           "Playlist_Id": result['Channel_data'][0]['Channel_Details']['Playlist_Id']
           }
       channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T
             
       # playlist data json to df
       playlist_tosql = {"Channel_Id": result['_id'],
                       "Playlist_Id": result['Channel_data'][0]['Channel_Details']['Playlist_Id']
                       }
       playlist_df = pd.DataFrame.from_dict(playlist_tosql, orient='index').T
    
       # video data json to df
       video_details_list = []
       for i in range(0,len(result['Channel_data'])):
           video_details_tosql = {
               'Playlist_Id':result['Channel_data'][0]['Channel_Details']['Playlist_Id'],
               'Video_Id': result['Channel_data'][1][f"video_Id_{i}"]['video_id'],
               'Video_Name': result['Channel_data'][1][f"video_Id_{i}"]['video_name'],
               'Video_Description': result['Channel_data'][1][f"video_Id_{i}"]['video_description'],
               'Published_date': result['Channel_data'][1][f"video_Id_{i}"]['publishedat'],
               'View_Count': result['Channel_data'][1][f"video_Id_{i}"]['view_count'],
               'Like_Count': result['Channel_data'][1][f"video_Id_{i}"]['like_count'],
               #'Dislike_Count': result['Channel_data'][1][f"video_Id_{i}"]['dislike_count'],
               'Favorite_Count': result['Channel_data'][1][f"video_Id_{i}"]['favorite_Count'],
               'Comment_Count': result['Channel_data'][1][f"video_Id_{i}"]['comment_count'],
               'Duration': result['Channel_data'][1][f"video_Id_{i}"]['duration'],
               'Thumbnail': result['Channel_data'][1][f"video_Id_{i}"]['thumbnail'],
               'Caption_Status': result['Channel_data'][1][f"video_Id_{i}"]['caption_status']
               }
           video_details_list.append(video_details_tosql)
       video_df = pd.DataFrame(video_details_list)
    
       # Comment data json to df
       Comment_details_list = []
       for i in range(0, len(result['Channel_data'])):
           comments_access = result['Channel_data'][1][f"video_Id_{i}"]['comments']
           if comments_access == 'Unavailable' or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access) :
               Comment_details_tosql = {
                   'Video_Id': 'Unavailable',
                   'Comment_Id': 'Unavailable',
                   'Comment_Text': 'Unavailable',
                   'Comment_Author':'Unavailable',
                   'Comment_Published_date': 'Unavailable',
                   }
               Comment_details_list.append(Comment_details_tosql)
               
           else:
               for j in range(1,3):
                   Comment_details_tosql = {
                   'Video_Id': result['Channel_data'][1][f"video_Id_{i}"]['video_id'],
                   'Comment_Id': result['Channel_data'][1][f"video_Id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_Id'],
                   'Comment_Text': result['Channel_data'][1][f"video_Id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_Text'],
                   'Comment_Author': result['Channel_data'][1][f"video_Id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_Author'],
                   'Comment_Published_date': result['Channel_data'][1][f"video_Id_{i}"]['comments'][f"Comment_Id_{j}"]['Comment_PublishedAt'],
                   }
                   Comment_details_list.append(Comment_details_tosql)
       Comments_df = pd.DataFrame(Comment_details_list)
       st.write(channel_df)
       connect = mysql.connector.connect(
       host = "localhost",
       user = "root",
       password = "BavaPreetha",
       database = "youtube_db1",      
       charset = "utf8mb4",
       auth_plugin = "mysql_native_password")
       # Create a new database and use
       mycursor = connect.cursor()
       for index, row in Comments_df.iterrows():
         sql="INSERT INTO comments (Video_Id,Comment_Id,Comment_Text,Comment_Author,Comment_Published_date) values (%s,%s,%s,%s,%s)"
         val = [
          row.Video_Id, row.Comment_Id,row.Comment_Text, row.Comment_Author, row.Comment_Published_date
         ]
         mycursor.execute(sql, val)
         connect.commit()
            
       for index1, row1 in channel_df.iterrows():  
         ch_sql="INSERT INTO channel (Channel_Name, Channel_Id, Video_Count, Subscriber_Count, Channel_Views, Channel_Description, Playlist_Id) values (%s,%s,%s,%s,%s,%s,%s)"
         ch_val = [
          row1.Channel_Name, row1.Channel_Id,row1.Video_Count, row1.Subscriber_Count, row1.Channel_Views, row1.Channel_Description, row1.Playlist_Id
         ]
         mycursor.execute(ch_sql, ch_val)
         connect.commit()
            
       for index2, row2 in playlist_df.iterrows():   
         pl_vi_sql="INSERT INTO playlist (Channel_Id, Playlist_Id) values (%s,%s)"
         pl_vi_val = [
          row2.Channel_Id,row2.Playlist_Id
         ]
         mycursor.execute(pl_vi_sql, pl_vi_val)
         connect.commit()
        
       for index3, row3 in video_df.iterrows():   
         vi_sql="INSERT INTO video (Playlist_Id,Video_Id, Video_Name ,Video_Description ,Published_date ,View_Count ,Like_Count ,Dislike_Count ,Favorite_Count ,Comment_Count ,Duration ,Thumbnail ,Caption_Status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
         vi_val = [
           row3.Playlist_Id,row3.Video_Id, row3.Video_Name ,row3.Video_Description ,row3.Published_date , row3.View_Count ,row3.Like_Count ,'0' ,row3.Favorite_Count ,row3.Comment_Count ,row3.Duration ,row3.Thumbnail ,row3.Caption_Status
         ]
         mycursor.execute(vi_sql, vi_val)
         connect.commit()        
       #mycursor.execute("CREATE TABLE channel (Channel_Name VARCHAR(255), Channel_Id VARCHAR(255),Video_Count INT, Subscriber_Count BigInteger, Channel_Views BigInteger, Channel_Description TEXT, Playlist_Id VARCHAR(255))")
       #mycursor.execute("CREATE TABLE playlist (Channel_Id VARCHAR(255), Playlist_Id VARCHAR(255))")
       # Close the cursor and database connection
       st.write('Migration Completed')
       mycursor.close()
       connect.close()
       # Connect to the new created database
    
with tab3:    
  question_tosql = st.selectbox('**Select your Question**',
  ('----select-------',
   '1. What are the names of all the videos and their corresponding channels?',
  '2. Which channels have the most number of videos, and how many videos do they have?',
  '3. What are the top 10 most viewed videos and their respective channels?',
  '4. How many comments were made on each video, and what are their corresponding video names?',
  '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
  '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
  '7. What is the total number of views for each channel, and what are their corresponding channel names?',
  '8. What are the names of all the channels that have published videos in the year 2022?',
  '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
  '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')
  
  # Creat a connection to SQL
  connect_for_question = pymysql.connect(host='localhost', user='root', password='BavaPreetha', db='youtube_db1')
  cursor = connect_for_question.cursor()
  
    
  # Q1
  if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
      cursor.execute("SELECT channel.Channel_Name, video.Video_Name FROM channel JOIN playlist JOIN video ON channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
      result_1 = cursor.fetchall()
      df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
      df1.index += 1
      st.dataframe(df1)
  
  # Q2
  elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
  
          cursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
          result_2 = cursor.fetchall()
          df2 = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
          df2.index += 1
          st.dataframe(df2)
  
  # Q3
  elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
  
          cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.View_Count DESC LIMIT 10;")
          result_3 = cursor.fetchall()
          df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
          df3.index += 1
          st.dataframe(df3)
  
  # Q4 
  elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
      cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
      result_4 = cursor.fetchall()
      df4 = pd.DataFrame(result_4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
      df4.index += 1
      st.dataframe(df4)
  
  # Q5
  elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
      cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
      result_5= cursor.fetchall()
      df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
      df5.index += 1
      st.dataframe(df5)
  
  # Q6
  elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
      st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
      cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Like_Count, video.Dislike_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
      result_6= cursor.fetchall()
      df6 = pd.DataFrame(result_6,columns=['Channel Name', 'Video Name', 'Like count','Dislike count']).reset_index(drop=True)
      df6.index += 1
      st.dataframe(df6)
  
  # Q7
  elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
  
          cursor.execute("SELECT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
          result_7= cursor.fetchall()
          df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
          df7.index += 1
          st.dataframe(df7)

  
  # Q8
  elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
      cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
      result_8= cursor.fetchall()
      df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
      df8.index += 1
      st.dataframe(df8)
  
  # Q9
  elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
      cursor.execute("SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
      result_9= cursor.fetchall()
      df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
      df9.index += 1
      st.dataframe(df9)
  
  # Q10
  elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
      cursor.execute("SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Comment_Count DESC;")
      result_10= cursor.fetchall()
      df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
      df10.index += 1
      st.dataframe(df10)
  
  # SQL DB connection close
  connect_for_question.close()
      
         # Video data to SQL
   