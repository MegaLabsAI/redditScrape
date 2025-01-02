
import openai
from openai import OpenAI






class Summarizer:
    def __init__(self):
       from app.aws import MyApp  # Import only when needed to avoid circular dependency
       self.app = MyApp()
      
       
      
       self.client = openai.OpenAI(api_key=self.app.aws['OPENAI_API_KEY'])
       self.default_prompt = """Summarize the following text: 
        Provide a concise summary, emphasizing the main points from the context."""



    def summarize_text(self, text, max_tokens=500):
        """Generates a summary for a given text using the OpenAI API."""
        prompt = f"{self.default_prompt} {text}"
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{
                    "role": "system",
                    "content": """
                        You will read given texts and summarize them. Focus on the main ideas, central themes, and key points, 
                        ensuring the essence of the text is captured clearly.
                        Avoid mentioning the author or text directly, and concentrate on distilling the core message and important insights.
                        Aim for brevity and clarity, regardless of the text's length.
                    """
                }, {
                    'role': 'user',
                    'content': prompt
                }],
                max_tokens=max_tokens,
                temperature=0.5,
                frequency_penalty=0.0,
                presence_penalty=0.6
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error summarizing text: {e}")
            return None
        
    

        
    # Define a function to summarize text chunks
    def summarize_text_chunks(self,text_chunks, max_tokens=1500):
        summaries = []
        for chunk in text_chunks:
            summary = self.summarize_text(chunk,  max_tokens=max_tokens)
            summaries.append(summary)
        return summaries

    # Define a function to split text into chunks
    def split_text_into_chunks(self,text, max_length):
        words = text.split()
        return [' '.join(words[i:i+max_length]) for i in range(0, len(words), max_length)]

    # Define a function to summarize text and handle long texts
    def summarize_long_text(self, text,  max_tokens=1500, max_chunk_length=4096):
        words = text.split()
        if len(words) <= max_chunk_length:
            return self.summarize_text(text, max_tokens=max_tokens)
        else:
            # Split the text into chunks
            chunks = self.split_text_into_chunks(text, max_chunk_length)
            # Summarize each chunk
            chunk_summaries = self.summarize_text_chunks(chunks, max_tokens=max_tokens)
            # Combine the summaries
            return ' '.join(chunk_summaries)

    # Assume df is your DataFrame and it has been loaded or created somewhere
    def final_sumamry(self, df):
        summaries = []

        for index, row in df.iterrows():
            text = str(row['body'])
            summary = self.summarize_long_text(text)
            summaries.append(summary)

        df['body_summary'] = summaries  # Adding the summaries to the DataFrame
    
        return df


    # def summarize_text_chunks(self, text_chunks, max_tokens=500):
    #     """Summarizes multiple text chunks."""
    #     summaries = []
    #     for chunk in text_chunks:
    #         summary = self.summarize_text(chunk, max_tokens=max_tokens)
    #         if summary:
    #             summaries.append(summary)
    #     return ' '.join(summaries)

    # def split_text_into_chunks(self, text, max_chunk_length=4096):
    #     """Splits text into manageable chunks."""
    #     words = text.split()
    #     return [' '.join(words[i:i + max_chunk_length]) for i in range(0, len(words), max_chunk_length)]

    # def summarize_long_text(self, text, max_tokens=500, max_chunk_length=4096):
    #     """Summarizes long texts by first splitting into chunks then summarizing each chunk."""
    #     if text.apply(lambda x: len(x.split()) if isinstance(x, str) else 0).max() <= max_chunk_length:
    #         return self.summarize_text(text, max_tokens=max_tokens)

    #     # if len(text.split()) <= max_chunk_length:
    #     #     return self.summarize_text(text, max_tokens=max_tokens)
    #     else:
    #         chunks = self.split_text_into_chunks(text, max_chunk_length)
    #         return self.summarize_text_chunks(chunks, max_tokens=max_tokens)



    