FROM jupyter/scipy-notebook:latest

RUN pip install --no-cache-dir \
    pyiceberg[s3fs,datafusion,pyiceberg-core]==0.11.1 \
    pyarrow>=14.0.0 \
    pandas>=2.0.0 \
    matplotlib \
    jupyterlab
