import joblib
import pandas as pd
import matplotlib.pyplot as plt
from xgboost import XGBClassifier
from catboost import CatBoostClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import os
import shap

class ModelTrainer:
    def __init__(self, df, target_col='target_20d', feature_cols=None):
        self.target_col = target_col
        self.df_all = df.copy()
        self.df_all['date'] = pd.to_datetime(self.df_all['date'])

        if target_col in df.columns:
            self.df_all = self.df_all.dropna(subset=[target_col]).sort_values('date')
        else:
            self.df_all = self.df_all.sort_values('date')
        
        if feature_cols:
            self.feature_cols = feature_cols
        else:
            # Exclude known non-feature columns
            exclude = [
                'key', 'ticker', 'date', 'market_sector', 'regime_id', 'regime_trend', 'regime_vol', 
                'Status', 'Alpha Score', 'signal_explanation', 'is_breakout', 'is_mean_reversion', 
                'is_rs_leader', 'is_accelerating', 'index', 'is_extended'
            ] + [c for c in df.columns if 'target' in c or 'future' in c]
            candidates = [c for c in df.columns if c not in exclude]
            self.feature_cols = [
                c for c in candidates
                if pd.api.types.is_numeric_dtype(self.df_all[c])
            ]

        if not self.feature_cols:
            raise ValueError("No numeric feature columns available for training.")

    @staticmethod
    def build_feature_matrix(df, feature_cols):
        """
        Align an input frame to the training feature list.

        Production data can miss a feature after an upstream fetch issue or schema
        change. Missing columns are added as NaN and handled by the model pipeline
        imputer, while unexpected columns are ignored.
        """
        X = df.copy()
        for col in feature_cols:
            if col not in X.columns:
                X[col] = pd.NA
        return X[feature_cols].apply(pd.to_numeric, errors="coerce")

    def get_models(self, pos_weight=1.0):
        xgb = Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
            ("model", XGBClassifier(
                n_estimators=200,
                learning_rate=0.05,
                max_depth=4,
                subsample=0.8,
                colsample_bytree=0.8,
                scale_pos_weight=pos_weight,
                random_state=42,
                eval_metric="logloss",
            )),
        ])
        
        cat = Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
            ("model", CatBoostClassifier(
                iterations=200,
                learning_rate=0.05,
                depth=4,
                random_seed=42,
                verbose=0,
                allow_writing_files=False,
            )),
        ])
        return {"XGBoost": xgb, "CatBoost": cat}

    def walk_forward_validation(self, model_name):
        df = self.df_all
        dates = sorted(df['date'].unique())
        n_splits = 5
        split_size = len(dates) // (n_splits + 1)

        if len(dates) < n_splits + 1 or split_size == 0:
            raise ValueError(
                f"Not enough dates for walk-forward validation: "
                f"{len(dates)} dates for {n_splits} splits."
            )
        
        all_test_results = []
        
        for i in range(n_splits):
            train_dates = dates[:(i+1)*split_size]
            test_dates = dates[(i+1)*split_size : (i+2)*split_size]
            
            train_df = df[df['date'].isin(train_dates)]
            test_df = df[df['date'].isin(test_dates)]

            if train_df.empty or test_df.empty:
                continue
            
            X_train = self.build_feature_matrix(train_df, self.feature_cols)
            y_train = train_df[self.target_col]
            X_test = self.build_feature_matrix(test_df, self.feature_cols)
            
            pos_weight = (len(y_train) - sum(y_train)) / sum(y_train) if sum(y_train) > 0 else 1.0
            model_dict = self.get_models(pos_weight)
            clf = model_dict[model_name]
            
            # Use Probability Calibration for better confidence
            cv = min(3, int(y_train.value_counts().min())) if y_train.nunique() > 1 else 0
            if cv < 2:
                raise ValueError(
                    f"Not enough samples per class to calibrate {model_name} "
                    f"on split {i + 1}: {y_train.value_counts().to_dict()}"
                )

            calibrated_clf = CalibratedClassifierCV(clf, cv=cv, method='sigmoid')
            calibrated_clf.fit(X_train, y_train)
            
            test_df = test_df.copy()
            test_df['probs'] = calibrated_clf.predict_proba(X_test)[:, 1]
            test_df['preds'] = (test_df['probs'] > 0.5).astype(int)
            
            all_test_results.append(test_df)

        if not all_test_results:
            raise ValueError("Walk-forward validation produced no test results.")

        final_test_df = pd.concat(all_test_results)
        model_dict = self.get_models()
        y_all = df[self.target_col]
        cv = min(3, int(y_all.value_counts().min())) if y_all.nunique() > 1 else 0
        if cv < 2:
            raise ValueError(
                f"Not enough samples per class to calibrate final {model_name}: "
                f"{y_all.value_counts().to_dict()}"
            )

        final_clf = CalibratedClassifierCV(model_dict[model_name], cv=cv, method='sigmoid')
        final_clf.fit(self.build_feature_matrix(df, self.feature_cols), y_all)
        
        return final_clf, final_test_df

    def evaluate_financials(self, df_test):
        """Run professional backtest on the walk-forward results."""
        from engine.backtester import ProfessionalBacktester
        bt = ProfessionalBacktester(df_test, tcosts=0.001)
        returns, metrics = bt.run()
        
        print(f"\n--- PROFESSIONAL BACKTEST (NET OF COSTS) ---")
        for k, v in metrics.items():
            print(f"{k}: {v}")
        return metrics

    def explain_model(self, model, X):
        """SHAP Interpretability."""
        try:
            X_sample = X.sample(min(500, len(X)))
            if hasattr(model, 'calibrated_classifiers_'):
                base_model = model.calibrated_classifiers_[0].estimator
            else:
                base_model = model

            if isinstance(base_model, Pipeline):
                imputer = base_model.named_steps["imputer"]
                fitted_model = base_model.named_steps["model"]
                X_sample = pd.DataFrame(
                    imputer.transform(X_sample),
                    columns=X_sample.columns,
                    index=X_sample.index,
                )
                base_model = fitted_model
                
            explainer = shap.TreeExplainer(base_model)
            shap_values = explainer.shap_values(X_sample)
            
            plt.figure(figsize=(10, 6))
            shap.summary_plot(shap_values, X_sample, show=False)
            plt.title("Feature Importance (SHAP)")
            plt.savefig("shap_summary.png")
            print("SHAP summary saved to shap_summary.png")
        except Exception as e:
            print(f"SHAP Error: {e}")

    def save_model(self, model, name):
        from src.config import MODELS_DIR
        path = os.path.join(MODELS_DIR, f"{name}.joblib")
        joblib.dump(model, path)
        print(f"Model saved to {path}")

    @staticmethod
    def load_model(name):
        from src.config import MODELS_DIR
        path = os.path.join(MODELS_DIR, f"{name}.joblib")
        if os.path.exists(path):
            return joblib.load(path)
        return None

def run_ml_experiment(dataset_path="dataset_signals.csv"):
    if not os.path.exists(dataset_path):
        print(f"File {dataset_path} not found.")
        return
    
    df = pd.read_csv(dataset_path)
    trainer = ModelTrainer(df)
    
    # Save the feature list for production use
    import joblib
    from src.config import MODELS_DIR
    joblib.dump(trainer.feature_cols, os.path.join(MODELS_DIR, "feature_list.joblib"))
    print(f"Feature list saved with {len(trainer.feature_cols)} columns.")
    
    # We'll use a simple ensemble of XGB and Cat
    results_list = []
    
    for name in ["XGBoost", "CatBoost"]:
        print(f"\n>>> AUDIT: Testing {name} with Walk-Forward + Calibration")
        model, results = trainer.walk_forward_validation(name)
        trainer.save_model(model, name)
        results_list.append(results[['key', 'probs']].rename(columns={'probs': f'probs_{name}'}))
    
    # Create Ensemble Probs
    ensemble_df = results_list[0].merge(results_list[1], on='key')
    ensemble_df['probs_ensemble'] = (ensemble_df['probs_XGBoost'] + ensemble_df['probs_CatBoost']) / 2
    
    # Merge back to original for evaluation
    final_results = df.merge(ensemble_df[['key', 'probs_ensemble']], on='key', how='inner')
    final_results['preds'] = (final_results['probs_ensemble'] > 0.5).astype(int)
    
    print("\n>>> FINAL BINARY ENSEMBLE RESULTS")
    from sklearn.metrics import classification_report
    print(classification_report(final_results[trainer.target_col], final_results['preds']))
    trainer.evaluate_financials(final_results)
    
    return trainer

if __name__ == "__main__":
    run_ml_experiment()
