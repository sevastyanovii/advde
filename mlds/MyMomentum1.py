import numpy as np
import matplotlib.pyplot as plt

N = 100
X = np.random.uniform(low=0, high=100, size=N)
Y = 2 * X + 1 + np.random.normal(scale=5, size=N)

# h(xi) = params[0] + params[1] * xi

EPOCHS = 5
LEARNING_RATE = 0.0001

costs = []
preds = []
params = np.random.normal(size=(2,))

change = [0.0, 0.0]
momentum = 0.3

for _ in range(EPOCHS):
    predictions = params[0] + params[1] * X
    preds.append(predictions)

    cost = np.sum(np.square(predictions - Y)) / (2 * len(predictions))
    costs.append(cost)

    change[0] = (momentum * change[0]) - LEARNING_RATE * np.sum(predictions - Y) / len(predictions)
    change[1] = (momentum * change[1]) - LEARNING_RATE * np.sum((predictions - Y) * X) / len(predictions)

    params[0] += change[0]
    params[1] += change[1]

    print(f"params {params[0]},  {params[0]}, {cost}")

plt.figure(figsize=(16,12))
plt.scatter(X, Y, color='blue')
plt.scatter(X, preds[4], color='red')
plt.show()
