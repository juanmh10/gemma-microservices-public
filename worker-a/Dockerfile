ARG VLLM_BASE_IMAGE=vllm/vllm-openai@sha256:ebc76cd4deee4d872adf2b081858144a39fba3df6eddd862e02902d7b580b821
FROM ${VLLM_BASE_IMAGE}
ARG VLLM_BASE_IMAGE
ENV WORKER_VLLM_BASE_IMAGE=${VLLM_BASE_IMAGE}

WORKDIR /app
COPY worker-a/requirements.lock /app/requirements.lock
RUN python3 -m pip install --no-cache-dir -r /app/requirements.lock
COPY worker-a/worker.py worker-a/compile_cache.py worker-a/domain.py /app/
COPY prompts/worker-a/v3.txt /app/prompt.txt

ARG GIT_SHA
LABEL org.opencontainers.image.revision=${GIT_SHA}
ENTRYPOINT ["python3", "/app/worker.py"]
CMD []
