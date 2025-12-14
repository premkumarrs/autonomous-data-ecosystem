import shap

def explain_model(model, X):
    explainer = shap.Explainer(model, X)
    shap_values = explainer(X)
    print("Model explainability computed.")
    return shap_values
