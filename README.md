# azbatch-manager
This is a utility that allows you to scale up your workloads using Azure Batch. It
assumes work is packaged in docker, and parametrised using envvars. It then lets you dispatch
work with with a few python calls by using the Azure Batch API under the hood. 

A discussion about whether this solution suits your purposes is found on xomnia.com

## Prerequisites
- A docker image that uses envvars to figure out what (chunk) to do
- An Azure Container registry that holds this image
- The `config.yaml` filled out (you also need some service principals etc.)
- Install the `requirements.txt`

## Getting started
You may want to test your workload (and integration to other resources) first. You can
then use the `dispatch-work-to-azbatch.ipynb` notebook to create a pool, a job and a
bunch of parametrized tasks. 
