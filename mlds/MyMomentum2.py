import numpy as np
import matplotlib.pyplot as plt

N = 100
X = np.random.uniform(low=0, high=100, size=N)
Y = 2 * X + 1 + np.random.normal(scale=5, size=N)

EPOCHS = 10
LEARNING_RATE = 0.0001

momentum = 0.3


def cost_function(X, y, theta0, theta1):
    total_cost = 0
    for i in range(len(X)):
        total_cost += (theta0 + theta1 * X[i] - y[i]) ** 2
    return total_cost / (2 * len(X))


def der_theta0(X, y, theta0, theta1):
    total_cost = 0
    for i in range(len(X)):
        total_cost += (theta0 + theta1 * X[i] - y[i])
    return total_cost / (len(X))


def der_theta1(X, y, theta0, theta1):
    total_cost = 0
    for i in range(len(X)):
        total_cost += (theta0 + theta1 * X[i] - y[i]) * X[i]
    return total_cost / (len(X))


theta0 = 1
theta1 = 1

preds = []

change = [0, 0]

for _ in range(EPOCHS):
    predictions = theta0 + theta1 * X
    preds.append(predictions)

    dt0 = der_theta0(X, Y, theta0, theta1)
    dt1 = der_theta1(X, Y, theta0, theta1)

    change[0] = (momentum * change[0]) - LEARNING_RATE * dt0
    change[1] = (momentum * change[1]) - LEARNING_RATE * dt1

    theta0 += change[0]
    theta1 += change[1]

    # print(f' change0={change[0]} change1={change[1]}')

    print("t0:", theta0, "t1:", theta1, "cost:", cost_function(X, Y, theta0, theta1))

plt.figure(figsize=(16,12))
plt.scatter(X, Y, color='blue')
plt.scatter(X, preds[9], color='red')
plt.show()
