

conda-update:
	conda env update --prune -f environment.yml

pip-tools:
	# pip install pip-tools
	python -m pip install pip-tools
	pip-compile requirements/prod.in && pip-compile requirements/dev.in

# execute after pull this repo
	pip-sync requirements/prod.txt requirements/dev.txt

reset:
	

train:
	python training/run_experiment.py --max_epochs=3 --gpus='1,' --data_class=CASIA --model_class=CNN
	
lint:
	tasks/lint.sh

# conda env rm CS3700-2022-2
# conda env remove -n CS3700-2022-2

