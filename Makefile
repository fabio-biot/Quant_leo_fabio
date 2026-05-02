run:
	python -m pipeline

clean:
	rm -f database/database.db
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__

pycache:
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__
	rm -rf XGBoost_model/__pycache__

re:
	rm -f database/database.db
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__
	python3 -m pipeline
	python3 -m XGBoost_model.dataset_builder
	rm -rf engine/__pycache__
	rm -rf src/__pycache__
	rm -rf XGBoost_model/__pycache__
	rm -rf __pycache__