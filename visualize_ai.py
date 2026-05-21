import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import os
from src.config import MODELS_DIR
from models.ml_pipeline import ModelTrainer

def generate_ai_visuals(dataset_path="dataset_signals.csv"):
    if not os.path.exists(dataset_path):
        print("Dataset not found.")
        return
    
    df = pd.read_csv(dataset_path)
    
    # 1. Feature Importance (from XGBoost)
    model = joblib.load(os.path.join(MODELS_DIR, "XGBoost.joblib"))
    
    # Access the base estimator if it's a CalibratedClassifierCV
    if hasattr(model, 'calibrated_classifiers_'):
        base_model = model.calibrated_classifiers_[0].estimator
    else:
        base_model = model

    if hasattr(base_model, "named_steps"):
        base_model = base_model.named_steps["model"]
        
    importances = base_model.feature_importances_
    features = joblib.load(os.path.join(MODELS_DIR, "feature_list.joblib"))
    
    feat_imp = pd.DataFrame({'feature': features, 'importance': importances})
    feat_imp = feat_imp.sort_values('importance', ascending=False).head(15)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='importance', y='feature', data=feat_imp, palette='viridis')
    plt.title("Intelligence Artificielle : Quelles 'Features' dictent les signaux ?", fontsize=14)
    plt.xlabel("Poids dans la décision")
    plt.ylabel("Indicateur Technique")
    plt.tight_layout()
    plt.savefig("reports/ai_feature_importance.png")
    print("Saved feature importance plot.")

    # 2. Probability Distribution
    # This shows if the model is 'sure' or 'hesitant'
    # We'll use the trained model on a sample of the data
    X = ModelTrainer.build_feature_matrix(df, features)
    probs = model.predict_proba(X)[:, 1]
    
    plt.figure(figsize=(10, 6))
    sns.histplot(probs, bins=50, kde=True, color='blue')
    plt.axvline(0.5, color='red', linestyle='--', label='Seuil de Décision (50%)')
    plt.axvline(0.6, color='green', linestyle='--', label='Zone de Haute Conviction (60%)')
    plt.title("Distribution des Probabilités (Alpha Scores)", fontsize=14)
    plt.xlabel("Confiance du Modèle (0 to 1)")
    plt.ylabel("Nombre de signaux générés")
    plt.legend()
    plt.savefig("reports/ai_probability_dist.png")
    print("Saved probability distribution plot.")

    # 3. Model Accuracy per Confidence Level
    # Does a 70% probability actually lead to more wins than 50%?
    df['prob'] = probs
    df['bin'] = pd.cut(df['prob'], bins=np.linspace(0, 1, 11))
    # 'target_20d' is our binary hit/miss
    win_rate = df.groupby('bin')['target_20d'].mean()
    
    plt.figure(figsize=(10, 6))
    win_rate.plot(kind='bar', color='skyblue')
    plt.axhline(df['target_20d'].mean(), color='red', linestyle='-', label='Moyenne du Marché')
    plt.title("Rigueur du Modèle : Le Win-Rate augmente-t-il avec la confiance ?", fontsize=14)
    plt.xlabel("Tranche de Probabilité")
    plt.ylabel("Taux de Réussite (Win-Rate)")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("reports/ai_calibration_curve.png")
    print("Saved calibration curve plot.")

if __name__ == "__main__":
    os.makedirs("reports", exist_ok=True)
    generate_ai_visuals()
