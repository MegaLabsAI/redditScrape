
import pandas as pd
from app.aws import MyApp




def main():
    app = MyApp()
    app.upload_data_to_s3()

if __name__ == "__main__":
    main()


