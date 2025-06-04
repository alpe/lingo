FROM docker.io/substratusai/vllm:v0.6.4.post1-cpu
COPY ./example/chat-template.jinja /tmp