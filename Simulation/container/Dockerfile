#FROM python:3.6.10-alpine
FROM python:3.7

# Install python and any other necessary dependencies
# awscli is necessary for the run_job.sh script to access S3 resources

# Copy the local folder to the Docker image
COPY ./ /usr/local/simulation_tutorial

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

RUN apt-get -y update && apt-get -y install gcc python-dev

RUN apt-get -y install curl

RUN pip3 install --no-cache-dir -r /usr/local/simulation_tutorial/requirements.txt

RUN curl https://certs.secureserver.net/repository/sf-class2-root.crt -o /usr/local/simulation_tutorial/sf-class2-root.crt



# Set the working directory to the newly created folder
WORKDIR /usr/local/simulation_tutorial
CMD ["script.py"]
ENTRYPOINT ["python3"]