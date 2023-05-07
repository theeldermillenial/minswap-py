import os
import blockfrost
from dotenv import load_dotenv

load_dotenv()
def api():

    
    api = blockfrost.BlockFrostApi(os.getenv("PROJECT_ID"))

    return api
