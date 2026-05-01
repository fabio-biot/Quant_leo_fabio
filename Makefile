run:
	python3 pipeline.py

clean:
	rm -f database/database.db
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__

pycache:
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__

re:
	rm -f database/database.db
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__
	python3 pipeline.py
	rm -rf __pycache__
	rm -rf engine/__pycache__
	rm -rf src/__pycache__