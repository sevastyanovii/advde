import random
from datetime import datetime, timedelta
import uuid
import json


def generate(iters=1000):
    vals = [{
        "uid": str(uuid.uuid1())
        , "dt": datetime.strftime(datetime.now() - timedelta(random.random()), "%Y-%m-%d %H:%m:%S")
        , "val": str(random.random() * 1000)
    } for _ in range(iters)]
    with open("./data.json", "w") as f:
        for val in vals:
            f.write(f"{json.dumps(val, indent=4)}\n\n")


if __name__ == "__main__":
    generate()

