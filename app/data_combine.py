import pandas as pd
import logging
class DataCombiner:
    def __init__(self):
        from app.aws import MyApp  # Avoid circular dependency by importing here
        self.app = MyApp()
        from app.summarizer import Summarizer 
        self.sum =   Summarizer()  # Import within the method to avoid circular import

     
    def fetch_new_reddit_data(self):
        """Fetch new Reddit submissions and top posts."""
        from app.reddit_data_extraction import RedditClient  # Avoid circular imports
        reddit_client = RedditClient()
        new_submissions_df = reddit_client.retrieve_submissions()
        new_top_df = reddit_client.retrieve_top_submissions()

        # Combine new submissions and top posts into a single DataFrame
        all_posts_df = pd.concat([new_submissions_df, new_top_df], ignore_index=True)
        return all_posts_df

    def combine_data(self):
       """Combine new Reddit data with existing data from S3, removing duplicates."""
       try:
            existing_df = self.app.retrieve_data_from_s3()
            new_data = self.fetch_new_reddit_data()
            existing_ids = self.app.load_existing_ids()
            

            # Ensure 'id' column exists
            if 'id' not in new_data.columns or 'id' not in existing_df.columns:
                raise ValueError("Missing 'id' column in new or existing data.")

            # Remove duplicates using 'id' check
            new_data = new_data[~new_data['id'].isin(existing_ids)].copy()

            if new_data.empty:
                logging.info("No new unique Reddit posts found. Nothing to save.")
                return existing_df  # Return existing data unchanged

            

            # Ensure numeric columns are cleaned
            for col in ['score', 'num_comments']:
                if col in new_data.columns:
                    new_data[col] = new_data[col].fillna(0).astype(int)
                if col in existing_df.columns:
                    existing_df[col] = existing_df[col].fillna(0).astype(int)

            # Convert subreddit column to string
            if 'subreddit' in new_data.columns:
                new_data['subreddit'] = new_data['subreddit'].astype(str)
            if 'subreddit' in existing_df.columns:
                existing_df['subreddit'] = existing_df['subreddit'].astype(str)

            df_summary =   self.sum.final_sumamry(new_data)

            # Append new and non-duplicate data
            combined_df = pd.concat([existing_df, df_summary], ignore_index=True)
            
            # Save updated IDs and hashes
            existing_ids.update(new_data['id'].tolist())
            
            self.app.save_ids_to_s3(existing_ids)
            

            logging.info(f"Successfully combined data. Total records: {len(combined_df)}")
            return combined_df

       except Exception as e:
            logging.error(f"Error in data combination: {e}")
            raise e




