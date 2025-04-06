import time
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from datetime import datetime, timedelta, timezone
import pandas as pd
import praw
import prawcore

class RedditClient:
    def __init__(self):
        from app.aws import MyApp
        self.app = MyApp()
        self.reddit = praw.Reddit(
            client_id=self.app.aws['red_client_id'],
            client_secret=self.app.aws['red_client_secret'],
            user_agent=self.app.aws['red_user_agent'],
            check_for_async=False
        )
        self.reddit_links = self.app.retrive_redlist_from_s3()

    def get_top_comments(self, post, n=3):
        try:
            post.comments.replace_more(limit=0)

            # Only keep comment objects with a .body attribute
            comment_bodies = [c for c in post.comments if hasattr(c, "body")]

            # Sort by score and pick top n
            top_comments = sorted(comment_bodies, key=lambda c: c.score, reverse=True)[:n]

            return "\n\n".join(c.body.strip() for c in top_comments)
        
        except Exception as e:
            logging.warning(f"Failed to fetch comments for post {post.id}: {e}")
            return ""


    def retrieve_posts(self, mode="hot", days_ago=1, max_retries=3):
        current_time = datetime.now(timezone.utc)
        time_cutoff = current_time - timedelta(days=days_ago)
        all_posts = []

        for topic in self.reddit_links:
            retries = 0
            while retries <= max_retries:
                try:
                    subreddit = self.reddit.subreddit(topic)
                    post_generator = getattr(subreddit, mode)(limit=1000)
                    subreddit_posts = []

                    for post in post_generator:
                        if post.selftext in [None, '[deleted]', '[removed]'] or post.stickied:
                            continue

                        created_time = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
                        if len(post.selftext) < 35 or created_time < time_cutoff:
                            continue

                        top_comments = top_comments = self.get_top_comments(post) if post.num_comments > 0 else ""


                        subreddit_posts.append({
                            'title': post.title,
                            'score': post.score,
                            'id': post.id,
                            'subreddit': str(post.subreddit),
                            'url': post.url,
                            'num_comments': post.num_comments,
                            'body': post.selftext,
                            'top_comments': top_comments,
                            'created': datetime.fromtimestamp(post.created, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                        })

                    all_posts.extend(subreddit_posts)
                    break  # Success

                except prawcore.exceptions.ServerError:
                    retries += 1
                    time.sleep(3)
                except Exception as e:
                    logging.error(f"Failed to retrieve data for subreddit {topic}. Error: {e}")
                    break

        return pd.DataFrame(all_posts)

    def retrieve_submissions(self, max_retries=3):
        return self.retrieve_posts(mode="hot", max_retries=max_retries)

    def retrieve_top_submissions(self, max_retries=3):
        return self.retrieve_posts(mode="top", max_retries=max_retries)
