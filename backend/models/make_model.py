import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import load_iris
from mlflow.models.signature import infer_signature
import pandas as pd

X, y = load_iris(return_X_y=True)
X = pd.DataFrame(X, columns=["f1","f2","f3","f4"])

model = LogisticRegression(max_iter=200)
model.fit(X, y)

signature = infer_signature(X, model.predict(X))

# Save a local MLflow model directory (this creates MLmodel, artifacts, etc.)
mlflow.sklearn.save_model(
    sk_model=model,
    path="my_mlflow_model",
    signature=signature
)

print("Saved MLflow model folder: my_mlflow_model/")
