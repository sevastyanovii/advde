import numpy as np
import matplotlib.pyplot as plt
from math import sqrt

N = 100
X = np.random.uniform(low=0, high=100, size=N)
Y = 2 * X + 1 + np.random.normal(scale=5, size=N)

# h(xi) = params[0] + params[1] * xi

EPOCHS = 50
LEARNING_RATE = 0.1

costs = []
preds = []
params = np.random.normal(size=(2,))

gradient = [0., 0.]
sq_grad_avg = [0., 0.]
rho = 0.99


for _ in range(EPOCHS):
    predictions = params[0] + params[1] * X
    preds.append(predictions)

    cost = np.sum(np.square(predictions - Y)) / (2 * len(predictions))
    costs.append(cost)

    gradient[0] = np.sum(predictions - Y) / len(predictions)
    sg = gradient[0] ** 2.0
    sq_grad_avg[0] = (sq_grad_avg[0] * rho) + (sg * (1.0 - rho))
    alpha = LEARNING_RATE / (1e-8 + sqrt(sq_grad_avg[0]))
    params[0] -= alpha * gradient[0]

    gradient[1] = np.sum((predictions - Y) * X) / len(predictions)
    sg = gradient[1] ** 2.0
    sq_grad_avg[1] = (sq_grad_avg[1] * rho) + (sg * (1.0 - rho))
    alpha = LEARNING_RATE / (1e-8 + sqrt(sq_grad_avg[1]))
    params[1] -= alpha * gradient[1]

    print(f"params {params[0]},  {params[1]}, cost={cost}")

plt.figure(figsize=(16, 12))
plt.scatter(X, Y, color='blue')
plt.scatter(X, preds[len(preds) - 1], color='red')
plt.show()
