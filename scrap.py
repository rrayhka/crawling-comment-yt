import pandas as pd
import os
from googleapiclient.discovery import build
from typing import List, Dict, Any

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def get_all_comments(
    youtube_api_key: str,
    video_id: str
) -> pd.DataFrame:
    """Scrape all comments for a YouTube video.

    Args:
        youtube_api_key: The YouTube API key.
        video_id: The ID of the YouTube video.

    Returns:
        A DataFrame containing the scraped comments.
    """
    youtube = build('youtube', 'v3', developerKey=youtube_api_key)
    data_video: List[List[Any]] = [
        ["Nama", "Komentar", "Waktu", "Likes", "Reply Count"]]

    param_comment = youtube.commentThreads().list(
        part="snippet", videoId=video_id, maxResults="100",
        textFormat="plainText")

    while True:
        data_comment: Dict[str, Any] = param_comment.execute()

        for i in data_comment["items"]:
            name: str = i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            comment: str = i["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            published_at: str = i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            likes: str = i["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            replies: int = i["snippet"]["totalReplyCount"]
            data_video.append([name, comment, published_at, likes, replies])

            if replies > 0:
                parent: str = i["snippet"]["topLevelComment"]["id"]
                param_replies = youtube.comments().list(
                    part="snippet", maxResults="100",
                    parentId=parent, textFormat="plainText")

                data_replies: Dict[str, Any] = param_replies.execute()

                for i in data_replies["items"]:
                    name: str = i["snippet"]["authorDisplayName"]
                    comment: str = i["snippet"]["textDisplay"]
                    published_at: str = i["snippet"]["publishedAt"]
                    likes: str = i["snippet"]["likeCount"]
                    replies: str = ""
                    data_video.append([name, comment, published_at, likes, replies])

            if 'nextPageToken' in data_comment:
                nextToken: str = data_comment['nextPageToken']
                param_comment = youtube.commentThreads().list_next(
                    param_comment, data_comment)
            else:
                break

    df = pd.DataFrame({
        "Nama": [i[0] for i in data_video],
        "Komentar": [i[1] for i in data_video],
        "Waktu": [i[2] for i in data_video],
        "Likes": [i[3] for i in data_video],
        "Reply Count": [i[4] for i in data_video]
    })

    df.to_csv("Hasil Scrape.csv", index=False, header=False)

    return df

