$ docker run --rm -v $PWD:/opt/notebooks -p 8888:8888 elopezdelara/miniconda3-spark jupyter notebook \
             --ip='*' --no-browser --allow-root --notebook-dir=/opt/notebooks \
             --NotebookApp.token=''