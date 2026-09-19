import time
import bz2
import os

def handler(params,context):
    mb_size = int(params["n"])
    bytes_to_generate = int(mb_size * 1024 * 1024)


    raw_data = os.urandom(bytes_to_generate)

    compressed_data = bz2.compress(raw_data)



    return {
        "status":"success"
    }