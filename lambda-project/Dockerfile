FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Install the binary packages using yum install
RUN yum install -y curl

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]