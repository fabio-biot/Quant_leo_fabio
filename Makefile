# Makefile for Quant Professional Stack

PYTHON=python3
VENV=venv
VENV_ACTIVATE=$(VENV)/bin/activate

.PHONY: install update-data build-dataset train-ml run-all clean today report backtest

install:
	$(PYTHON) -m venv $(VENV)
	. $(VENV_ACTIVATE) && pip install -r requirements.txt
	. $(VENV_ACTIVATE) && pip install xgboost lightgbm catboost torch scikit-learn matplotlib

update-data:
	. $(VENV_ACTIVATE) && $(PYTHON) pipeline.py

build-dataset:
	. $(VENV_ACTIVATE) && $(PYTHON) -c "from engine.engineer import build_final_dataset, save_final_dataset; df=build_final_dataset(); save_final_dataset(df)"

train-ml:
	. $(VENV_ACTIVATE) && $(PYTHON) -c "from models.ml_pipeline import run_ml_experiment; run_ml_experiment()"

today:
	. $(VENV_ACTIVATE) && $(PYTHON) -m engine.production

report: today

run-all: update-data build-dataset train-ml today

backtest:
	@if [ -z "$(TICKER)" ]; then echo "Usage: make backtest TICKER=AAPL"; exit 1; fi
	@if [ ! -f dataset_signals.csv ]; then echo "Missing dataset_signals.csv. Run: make build-dataset"; exit 1; fi
	@echo "Running backtest for ticker $(TICKER)"
	. $(VENV_ACTIVATE) && $(PYTHON) visualize_backtest.py --ticker $(TICKER)


clean:
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__
	rm -rf models/__pycache__
	rm -f dataset_signals.csv
