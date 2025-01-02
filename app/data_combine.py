
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq



class DataCombiner:
    def __init__(self):
        from app.aws import MyApp  # Import only when needed to avoid circular dependency
        self.app = MyApp()
    #    self.existing_df = self.app.retrieve_data_from_s3()
    #    self.data = RedditClient()

       
    def fetch_new_reddit_data(self):
        """Fetch new Reddit submissions and top posts."""
        from app.reddit_data_extraction import RedditClient  # Import within method to avoid circular import
        reddit_client = RedditClient()
        new_submissions_df = reddit_client.retrieve_submissions()
        new_top_df =   reddit_client.retrieve_top_submissions()

        # Combine the new submissions and top posts into a single DataFrame
        all_posts_df = pd.concat([new_submissions_df, new_top_df], ignore_index=True)
        return all_posts_df

    #    self.new_submissions_df = self.data.retrieve_submissions()
    #    self.new_top_df = self.data.retrieve_top_submissions()
       
    #    self.all_posts_df = pd.concat([self.new_submissions_df, self.new_top_df], ignore_index=True)


    def combine_data(self):
        """Combine new Reddit data with existing data in S3, removing duplicates."""
        try:
            # Step 1: Read existing data from S3
            existing_df = self.app.retrieve_data_from_s3()
            new_data = self.fetch_new_reddit_data()

        except Exception as e:
            print(f"Error in data combination: {e}")
            raise e
        try:
            # Step 2: Ensure the new data's 'score' column is cleaned up
            new_data['score'] = new_data['score'].fillna(0).apply(lambda x: round(x))
            existing_df['score'] = existing_df['score'].fillna(0).apply(lambda x: round(x))
        except Exception as e:
            print(f"Error in data combination: {e}")
            raise e
        try:
            # Step 3: Ensure the 'subreddit' column is a string
            new_data['subreddit'] = new_data['subreddit'].astype(str)
            existing_df['subreddit'] = existing_df['subreddit'].astype(str)
        except Exception as e:
            print(f"Error in data combination: {e}")
            raise e
        try:
            # Step 4: Ensure the 'num_comments' column is an integer
           
           # Fill NaN values with 0 (or another value), then convert to int
            new_data['num_comments'] = new_data['num_comments'].fillna(0).astype(int).round(5)
            existing_df['num_comments'] = existing_df['num_comments'].fillna(0).astype(int).round(5)

        except Exception as e:
            print(f"Error in data combination: {e}")
            raise e
        try:

            # Step 5: Identify duplicates based on 'id' and 'body' columns
            existing_ids = set(existing_df['id'])
            new_ids = set(new_data['id'])
            duplicate_ids = existing_ids.intersection(new_ids)

            existing_bodies = set(existing_df['body'])
            new_bodies = set(new_data['body'])
            duplicate_bodies = existing_bodies.intersection(new_bodies)

            # Step 6: Filter out rows where both 'id' and 'body' match existing data
            filtered_df = new_data[
                ~(new_data['id'].isin(duplicate_ids) & new_data['body'].isin(duplicate_bodies))
            ]
            filtered_df.loc[:, 'score'] = filtered_df['score'].apply(lambda x: round(x))

           
            return filtered_df

        except Exception as e:
            print(f"Error in data combination: {e}")
            raise e

    

    # def combine_data(self):

       
    #     """Combine new Reddit data with existing data in S3, removing duplicates."""
    #     try:
    #         # Step 1: Read existing data from S3
    #         existing_df = self.app.retrieve_data_from_s3()

    #         # Step 2: Combine new submissions and top submissions
            

    #         # Step 3: Handle any missing or NaN values in the 'score' column
    #         self.all_posts_df['score'] = self.all_posts_df['score'].fillna(0).apply(lambda x: round(x))
    #         existing_df['score'] = existing_df['score'].fillna(0).apply(lambda x: round(x))

    #         # Step 4: Ensure 'subreddit' column is of type string
    #         self.all_posts_df['subreddit'] = self.all_posts_df['subreddit'].astype(str)
    #         existing_df['subreddit'] = existing_df['subreddit'].astype(str)

    #         # Step 5: Ensure 'num_comments' column is in correct precision format (as integer)
    #         self.all_posts_df['num_comments'] = self.all_posts_df['num_comments'].astype(int).round(5)
    #         existing_df['num_comments'] = existing_df['num_comments'].astype(int).round(5)

    #         # Step 6: Identify duplicates based on 'id' and 'body' columns
    #         existing_ids = set(existing_df['id'])
    #         new_ids = set(self.all_posts_df['id'])
    #         duplicate_ids = existing_ids.intersection(new_ids)

    #         existing_bodies = set(existing_df['body'])
    #         new_bodies = set(self.all_posts_df['body'])
    #         duplicate_bodies = existing_bodies.intersection(new_bodies)

    #         # Step 7: Filter out rows where both 'id' and 'body' match existing data
    #         filtered_df = self.all_posts_df[
    #             ~(self.all_posts_df['id'].isin(duplicate_ids) & self.all_posts_df['body'].isin(duplicate_bodies))
    #         ]
            

    #         return filtered_df

    #     except Exception as e:
    #         print(f"Error in data combination: {e}")
    #         raise e
        

    