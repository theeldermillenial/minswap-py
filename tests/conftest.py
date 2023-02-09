import blockfrost
from dotenv import dotenv_values


def api():

    env = dotenv_values()
    api = blockfrost.BlockFrostApi(env["PROJECT_ID"])

    return api
