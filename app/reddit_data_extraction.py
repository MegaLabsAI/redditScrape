
import praw
import boto3
import time

import prawcore
import pandas as pd
import json
from datetime import datetime, timedelta
# from app.aws import MyConfig, MyApp





class RedditClient:
    def __init__(self):
        from app.aws import MyApp  # Import within method to avoid circular import
        self.app = MyApp()
        self.reddit = praw.Reddit(
            client_id= self.app.aws['red_client_id'],
            client_secret= self.app.aws['red_client_secret'],
            user_agent=self.app.aws['red_user_agent'],
            check_for_async=False
        )
        self.reddit_links = self.app.retrive_redlist_from_s3()

        


    def retrieve_submissions(self,   days_ago=1,max_retries=3):
        current_time = datetime.now()
        seven_days_ago = current_time - timedelta(days_ago)
        all_posts = []
        

        for topic in self.reddit_links:
            retries = 0
            while retries <= max_retries:
                try:
                    ml_subreddit = self.reddit.subreddit(topic)
                    subreddit_posts = []

                    for post in ml_subreddit.hot(limit=100000):
                        if post.selftext in [None, '[deleted]', '[removed]'] or post.stickied:
                            continue

                        if len(post.selftext) >= 35 and datetime.utcfromtimestamp(post.created_utc) >= seven_days_ago:
                            time.sleep(2)

                            subreddit_posts.append({
                                'title': post.title,
                                'score': post.score,
                                'id': post.id,
                                'subreddit': str(post.subreddit),
                                'url': post.url,
                                'num_comments': post.num_comments,
                                'body': post.selftext,
                                'created': datetime.utcfromtimestamp(post.created).strftime('%Y-%m-%d %H:%M:%S')
                                
                            })

                    all_posts.extend(subreddit_posts)
                    break  # Break out of the retry loop on success

                except prawcore.exceptions.ServerError:
                    retries += 1
                    time.sleep(3)  # Wait before retrying
                except Exception as e:
                    print(f"Failed to retrieve data for subreddit {topic}. Error: {e}")
                    break  # Skip to the next subreddit

        return pd.DataFrame(all_posts)

    def retrieve_top_submissions(self, max_retries=3):


        # This would be similar to retrieve_submissions but uses the subreddit.top() method
        current_time = datetime.now()
        seven_days_ago = current_time - timedelta(days=1)
        all_posts = []

        for topic in self.reddit_links:
            retries = 0
            while retries <= max_retries:
                try:
                    ml_subreddit = self.reddit.subreddit(topic)
                    subreddit_posts = []

                    for post in ml_subreddit.top(limit=100000):
                        if post.selftext in [None, '[deleted]', '[removed]'] or post.stickied:
                            continue

                        if len(post.selftext) >= 35 and datetime.utcfromtimestamp(post.created_utc) >= seven_days_ago:
                            time.sleep(2)
                

                            subreddit_posts.append({
                                'title': post.title,
                                'score': post.score,
                                'id': post.id,
                                'subreddit': str(post.subreddit),
                                'url': post.url,
                                'num_comments': post.num_comments,
                                'body': post.selftext,
                                'created': datetime.utcfromtimestamp(post.created).strftime('%Y-%m-%d %H:%M:%S')
                            
                            })

                    all_posts.extend(subreddit_posts)
                    break  # Break out of the retry loop on success

                except prawcore.exceptions.ServerError:
                    retries += 1
                    time.sleep(3)  # Wait before retrying
                except Exception as e:
                    print(f"Failed to retrieve data for subreddit {topic}. Error: {e}")
                    break  # Skip to the next subreddit

        return pd.DataFrame(all_posts)