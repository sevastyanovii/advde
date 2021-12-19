import json
import random
from datetime import datetime, timedelta

a = ['aaa', 'bbb', 'ccc']
b = ['m1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9', 'm10']


def generate(iters=1000):
    vals = [{
        "id": a[round(random.random() * 10) % 3]
        , "dt": datetime.strftime(datetime.now() - timedelta(random.random()), "%Y-%m-%d %H:%m:%S")
        , "metric": b[round(random.random() * 10) % 10]
        , "val": str(random.random() * 1000)
    } for _ in range(iters)]
    with open("./data2.json", "w") as f:
        for val in vals:
            f.write(f"{json.dumps(val, indent=4)}\n\n")


if __name__ == "__main__":
    generate()
