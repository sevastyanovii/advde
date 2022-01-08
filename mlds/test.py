import numpy as np

a = np.asarray([1, 2, 3])
b = 1 + a * 2

print(b)

a = [1, 2, 3]
b = [4, 5, 6]

for i in zip(a, b):
    print(i)
