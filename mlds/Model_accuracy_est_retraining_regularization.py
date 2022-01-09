import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier

data = sm.datasets.fair.load_pandas().data
# print(data.info())

X = data[data.columns[:-1]]
# X.head(2)

Y = (data['affairs'] > 0).astype(int)
# Y = Y['affairs']
# Y.info()

model = LogisticRegression()
model_tree = DecisionTreeClassifier()

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3, random_state=0)
# X_train, Y_train

model.fit(X_train, Y_train)
model_tree.fit(X_train, Y_train)

predictions = model.predict_proba(X_test)

# predictions[:2]

print(model.score(X_train, Y_train))
print(model.score(X_test, Y_test))
print('model_tree train ', model_tree.score(X_train, Y_train))
print('model_tree test ', model_tree.score(X_test, Y_test))

tp = 0  # True positive
fp = 0  # False positive
fn = 0  # False negative
tn = 0  # True negative

predictions = model.predict_proba(X_test)
for predicted_prob, actual in zip(predictions[:, 1], Y_test):
    if predicted_prob >= 0.5:
        predicted = 1
    else:
        predicted = 0

    if predicted == 1:
        if actual == 1:
            tp += 1
        else:
            fp += 1

    else:
        if actual == 1:
            fn += 1
        else:
            tn += 1

pred = model.predict(X_test)

print(tp, fp, fn, tn)

, 'temperature', 'pressure', 'light', 'air_humidity', 'volume'
