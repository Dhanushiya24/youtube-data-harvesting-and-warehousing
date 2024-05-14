from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
from datetime import datetime
import streamlit as st

#api  key connection

def api_connect():
    api_id="AIzaSyD_WrIJwWWfSSKZ7FTUgnLDk2UKlh3PdD4"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey= api_id)
    return youtube

youtube=api_connect()

st.set_page_config(layout="wide")
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data collection")
    st.caption("API Integration")
    st.caption("Data Management using SQL")

channel_ids=st.text_input("Enter the channel ID")
# if channel_ids:
#     st.write(get_channel_info(channel_ids))

#get channels information

def get_channel_info(channel_ids):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_ids
    )
    response=request.execute()

    
    if 'items' in response and response['items']:
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['title'],
                    Channel_Id=i['id'],
                    Subscribes=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Description=i['snippet']['description'],
                    Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])

            return data  

channel_details=get_channel_info(channel_ids)
channel_details_df = pd.DataFrame(channel_details,index=[0])
channel_details_df

# channel_ids=st.text_input("Enter the channel ID")
if channel_ids:
    st.write(get_channel_info(channel_ids))

    

    #get video ids

def get_videos_ids(channel_ids):
    video_ids=[]
    response=youtube.channels().list(id=channel_ids,
                                    part='contentDetails').execute()
    if 'items' in response and response['items']:
        playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


        next_page_token=None            #max is 50,so that we using next_page_token

        while True: #to get total video
            response1=youtube.playlistItems().list(             #playlistitem=to get playlist videos
                                                part='snippet',#to get videoid
                                                playlistId=playlist_Id,
                                                maxResults=50,      #maxresult is to get the 50 videos
                                                pageToken=next_page_token).execute()        #pagetoken is used to go one page to next page
            for i in range(len(response1['items'])): #for loop is  reach the end till it run
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')#in this next_page_token is used to take another 50 and it reach 50 then go to pagetoken again it run till reach the end

            if next_page_token is None:
                break
    return video_ids

video_ids=get_videos_ids(channel_ids)


#get video information


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    video_Id=item['id'],
                    Title=item['snippet']['title'],
                    #Tags=item['snippet']['tags']why we using get function here is somebody off the commentbox,view,tags so thatwe using get functions and we remove list bracket and we use function bracket()
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet']['description'],
                    Published_date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

video_details=get_video_info(video_ids)
video_details_df=pd.DataFrame(video_details)
video_details_df

#get comment info

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                        part='snippet',
                        videoId=video_id,
                        maxResults=50
                    )
        response=request.execute()

        for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        
                Comment_data.append(data)
    except:
        pass

    return Comment_data

comment_details=get_comment_info(video_ids)
comment_details_df=pd.DataFrame(comment_details)
comment_details_df


#create table for channels


mydb=mysql.connector.connect(host="localhost",
                            user="root",
                            password="root",
                            database="youtube_data",
                            port="3306")

mycursor=mydb.cursor()
def channel_table():
    try:
        create_query=('''create table if not exists channels(Channel_Name varchar(100),
                                                Channel_Id varchar(80) primary key,
                                                Subscribes bigint,
                                                Views bigint,
                                                Total_Videos int,
                                                Channel_Description text,
                                                Playlist_Id varchar(80))''')

        mycursor.execute(create_query)
        mydb.commit()

    except:
        print("channels values are already inserted")
channel_table()

    


for row in channel_details_df.itertuples(index=False):
    insert_query='''insert into channels(Channel_Name,
                            Channel_Id,
                            Subscribes,
                            Views,
                            Total_Videos,
                            Channel_Description,
                            Playlist_Id)
                            
                            values(%s,%s,%s,%s,%s,%s,%s)'''
    values = (
            row.Channel_Name,
            row.Channel_Id,
            row.Subscribes,
            row.Views,
            row.Total_Videos,
            row.Channel_Description,
            row.Playlist_Id
        )
        
    try:
        mycursor.execute(insert_query,values)
        mydb.commit()

    except:
        print("channels values are already inserted")



    #create table for videos


mydb=mysql.connector.connect(host="localhost",
                        user="root",
                        password="root",
                        database="youtube_data",
                        port="3306")

mycursor=mydb.cursor()


def videos_table():
    create_query='''create table if not exists videos(channel_Name varchar(100),
                                                            channel_Id varchar(100),
                                                                video_Id varchar(30),
                                                                Title varchar(150),
                                                                Thumbnail varchar(200),
                                                                Description text,
                                                                Published_date timestamp,
                                                                Duration varchar(30),
                                                                Views bigint,
                                                                Likes bigint,
                                                                Comments int,
                                                                Favourite_count int,
                                                                Definition varchar(10),
                                                                Caption_status varchar(50)
                                                                )'''
    mycursor.execute(create_query)
    mydb.commit()
videos_table()



def convert_to_mysql_datetime(dt_str):
    dt = datetime.fromisoformat(dt_str.replace('Z', ''))
    return dt.strftime('%Y-%m-%d %H:%M:%S')



# Corrected code to use the converted datetime
for row in video_details_df.itertuples(index=False):
    insert_query = '''INSERT INTO videos(
                                        channel_Name,
                                        channel_Id,
                                        video_Id,
                                        Title,
                                        Thumbnail,
                                        Description,
                                        Published_date,
                                        Duration,
                                        Views,
                                        Likes,
                                        Comments,
                                        Favourite_count,
                                        Definition,
                                        Caption_status ) 
                                        
                                        Values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    published_date_mysql = convert_to_mysql_datetime(row.Published_date)

    values = (
        row.channel_Name,
        row.channel_Id,
        row.video_Id,
        row.Title,
        row.Thumbnail,
        row.Description,
        published_date_mysql,  
        row.Duration,
        row.Views,
        row.Likes,
        row.Comments,
        row.Favourite_count,
        row.Definition,
        row.Caption_status
    )
    
    mycursor.execute(insert_query, values)
    mydb.commit()


# create table for comment


mydb=mysql.connector.connect(host="localhost",
                        user="root",
                        password="root",
                        database="youtube_data",
                        port="3306")

mycursor=mydb.cursor()

def comment_table():
    create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                            Video_id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published timestamp)'''

                        

    mycursor.execute(create_query)
    mydb.commit()
comment_table()



def convert_to_mysql_datetime(dt_str):
    dt = datetime.fromisoformat(dt_str.replace('Z', ''))
    return dt.strftime('%Y-%m-%d %H:%M:%S')

 
for row in comment_details_df.itertuples(index=False):
    
    comment_published_mysql = convert_to_mysql_datetime(row.Comment_Published)

    insert_query = '''INSERT INTO comments(
                        Comment_Id,
                        Video_id,
                        Comment_Text,
                        Comment_Author,
                        Comment_Published) 
                    VALUES (%s, %s, %s, %s, %s)'''

    values = (
        row.Comment_Id,
        row.Video_id,
        row.Comment_Text,
        row.Comment_Author,
        comment_published_mysql  
    )

    
    mycursor.execute(insert_query, values)
    mydb.commit()


def tables():
    channel_table()
    videos_table()
    comment_table()

    return "table created successfully"

def show_channels():

    channel_details_df = pd.DataFrame(channel_details,index=[0])
    channel_details_df


    return channel_details_df

def show_videos():

    video_details_df=pd.DataFrame(video_details)
    video_details_df

    return video_details_df

def show_comments():

    comment_details_df=pd.DataFrame(comment_details)
    comment_details_df

    return comment_details_df

def details(channel_ids):
    return show_channels(),show_videos(),show_comments()

#streamlit part




if st.button("collect and store data") and channel_ids:
    insert=details(channel_ids)
        # ch_ids=[]
        # channel_details_df =channel_ids
        # ch_ids.append(channel_details_df)

        # if channel_ids in ch_ids:
            # st.success("channel deatils of the given channel id already exists")

        # else:
            
            # st.success(insert)


if st.button("travel to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels()


elif show_table=="VIDEOS":
    show_videos()
    

elif show_table=="COMMENTS":
    show_comments()    


#SQL CONNECTION 

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtube_data",
    port="3306"
)

mycursor = mydb.cursor()
question = st.selectbox("Select your question", ("1. All the videos and the channel name",
                                                "2. channels with most number of videos",
                                                "3. 10 most viewed videos and their respective channels",
                                                "4. comments in each videos",
                                                "5. videos with highest likes",
                                                "6. likes of all videos",
                                                "7. views of each channel",
                                                "8. videos published in the year of 2022",
                                                "9. average duration of all videos in each channels",
                                                "10. videos with highest number of comments"))

if question == "1. All the videos and the channel name":
    query1 = '''SELECT title AS video_title, channel_name AS channel_name FROM videos'''
    mycursor.execute(query1)
    t1 = mycursor.fetchall()
    df = pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)

elif question == "2. channels with most number of videos":
    query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels
                ORDER BY total_videos DESC'''
    mycursor.execute(query2)
    t2 = mycursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos and their respective channels":
    query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos
                WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
    mycursor.execute(query3)
    t3 = mycursor.fetchall()

    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif question == "4. comments in each videos":
    query4 = '''SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL'''
    mycursor.execute(query4)
    t4 = mycursor.fetchall()

    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question == "5. videos with highest likes":
    query5 = '''SELECT title AS videotitle, channel_name AS channelname, likes AS likecount 
                FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
    mycursor.execute(query5)
    t5 = mycursor.fetchall()

    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question == "6. likes of all videos":
    query6 = '''SELECT likes AS likecount, title AS videotitle FROM videos'''
    mycursor.execute(query6)
    t6 = mycursor.fetchall()

    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. views of each channel":
    query7 = '''SELECT channel_name AS channelname, views AS totalviews FROM videos'''
    mycursor.execute(query7)
    t7 = mycursor.fetchall()

    df7 = pd.DataFrame(t7, columns=["channel name", "totalviews"])
    st.write(df7)

elif question == "8. videos published in the year of 2022":
    query8 = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname 
                FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022'''
    mycursor.execute(query8)
    t8 = mycursor.fetchall()

    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question == "9. average duration of all videos in each channels":
    query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration 
                FROM videos GROUP BY channel_name'''
    mycursor.execute(query9)
    t9 = mycursor.fetchall()

    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])
    st.write(df9)

elif question == "10. videos with highest number of comments":
    query10 = '''SELECT title AS videotitle, channel_name AS channelname, comments 
                 FROM videos WHERE comments IS NOT NULL 
                 ORDER BY comments DESC'''
    mycursor.execute(query10)

    t10 = mycursor.fetchall()

    df10 = pd.DataFrame(t10, columns=["videotitle", "channelname", "comments"])
    st.write(df10)




