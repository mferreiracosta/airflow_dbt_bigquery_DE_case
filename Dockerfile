FROM quay.io/astronomer/astro-runtime:12.1.0


# Install dask into a virtual env
RUN python -m venv dask_venv && source dask_venv/bin/activate && \
    pip install --no-cache-dir "dask[complete]" pandas gcsfs && deactivate
