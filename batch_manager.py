import datetime
import logging
import time
from typing import Any, Dict, Optional

import azure.batch._batch_service_client as batch  # noqa
import yaml
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def read_config(filename='config.yaml'):
    with open(filename) as f:
        config = yaml.safe_load(f)

    return config

logger = logging.getLogger(__name__)

NODE_AGENT_SKU_ID = "batch.node.ubuntu 20.04"
VM_IMAGE_REFERENCE = batch.models.ImageReference(
    publisher="microsoft-azure-batch",
    offer="ubuntu-server-container",
    sku="20-04-lts",
    version="latest",
)
""" you need a '...-container' offer from 'microsoft-azure-batch' publisher in
order to execute docker containers on azure batch.
"""


class AzureBatchManager:
    def __init__(self, credential: Optional[Any] = None) -> None:
        """Azure Batch manager, which provides some simple functions to help
        interacting with the Azure Batch Service.

        This manager connects to the container registry specified.

        Parameters
        ----------
        credential: Optional[Any]
            Credential to provide access to the azure services. If not provided, it
            will use the DefaultAzureCredential.
        """
        self.config = read_config()
        self.credential = credential or DefaultAzureCredential()
        self.secret_client = SecretClient(
            self.config["azure"]["key_vault"]["account_url"], self.credential
        )

        batch_creds = ServicePrincipalCredentials(
            client_id=self.config["azure"]["batch_sp"]["client_id"],
            secret=self.secret_client.get_secret(
                self.config["azure"]["batch_sp"]["kv_secret_key"]
            ).value,
            tenant=self.config["azure"]["batch_sp"]["tenant_id"],
            resource=self.config["azure"]["batch_sp"]["resource"],
        )

        self.batch_client = batch.BatchServiceClient(
            batch_creds, self.config["azure"]["batch"]["account_url"]
        )

    def create_pool(
        self,
        pool_id: str,
        docker_image: str,
        vm_type="STANDARD_D1_V2",
        low_prio_nodes: int = 5,
        dedicated_nodes: int = 0,
    ) -> None:
        """Creates an Azure Batch pool of docker ready vms.

        Parameters
        ----------
        pool_id: str
            id for the pool to be created.
        docker_image: str
            docker image (including tag and available in our registry) you want to run
            on the vms in the pool. This image will be pre-fetched in the pool, which
            makes running the task faster.
        vm_type: str
            Any VM type available in our region on Azure. Defaults to STANDARD_D1_V2
        low_prio_nodes: int
            Number of low priority nodes in the pool. Preferred over dedicated nodes
            due to lower costs.
        dedicated_nodes: int
            Number of dedicated nodes in the pool. Use only when necessary due to
            higher costs than low priority nodes.

        Returns
        -------

        """

        container_registry = batch.models.ContainerRegistry(
            registry_server=self.config["azure"]["acr"]["server"],
            user_name=self.config["azure"]["acr"]["user_name"],
            password=self.secret_client.get_secret(
                self.config["azure"]["acr"]["kv_secret_key"]
            ).value,
        )

        # pre-fetching the image in the pool, you can still use other images in a task,
        # but they will be downloaded (pulled) at runtime per task.
        container_conf = batch.models.ContainerConfiguration(
            container_image_names=[
                f"{self.config['azure']['acr']['server']}/{docker_image}",
            ],
            container_registries=[container_registry],
        )

        vmconfig = batch.models.VirtualMachineConfiguration(
            image_reference=VM_IMAGE_REFERENCE,
            container_configuration=container_conf,
            node_agent_sku_id=NODE_AGENT_SKU_ID,
        )

        # Ensuring the pool is created within the virtual network corresponding to this
        # RG. Without this vnet_config, the VM's in the pool are not allowed to access
        # data from a database.
        subnet_id = self.config["vnet-azure-batch"]["subnet-id"]
        vnet_config = batch.models.NetworkConfiguration(subnet_id=subnet_id)

        new_pool = batch.models.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=vmconfig,
            vm_size=vm_type,
            target_dedicated_nodes=dedicated_nodes,
            target_low_priority_nodes=low_prio_nodes,
            network_configuration=vnet_config,
        )
        self.batch_client.pool.add(new_pool)

    def delete_pool(
        self,
        pool_id: str,
        force: bool = False,
        inc_associated_jobs: bool = True,
    ) -> None:
        """Deletes an Azure Batch pool given the `pool_id`

        Parameters
        ----------
        pool_id: str
            id of the pool to delete.
        force: bool
            Whether or not to force the deletion, even when a task/job is not completed
            yet. Defaults to False.
        inc_associated_jobs: bool
            Whether or not to delete tasks/jobs associated with the pool. This is
            recommended since we store confidential information in the job environment
            variables. Defaults to True.
        """
        jobs_in_pool = [
            j for j in self.batch_client.job.list() if j.pool_info.pool_id == pool_id
        ]
        all_jobs_completed = all([self.is_job_completed(j.id) for j in jobs_in_pool])
        if not force and not all_jobs_completed:
            raise RuntimeError(
                "ERROR: Not all Jobs (tasks within jobs) reach 'Completed' state. If "
                "you still want to delete the pool, use `force=True`."
            )
        else:
            # Associated jobs are not automatically deleted when deleting the pool...
            if inc_associated_jobs:
                for j in jobs_in_pool:
                    self.delete_job(j.id, force=force)

            self.batch_client.pool.delete(pool_id)

    def create_job(self, job_id: str, pool_id: str) -> None:
        """Creates a job for a specific pool. A job is a wrapper for one or multiple
        tasks.

        Notes
        -----
        You may want to set some envvars here that are used by every task.

        Parameters
        ----------
        job_id: str
            job id for the Azure Batch job. This name must be unique.
        pool_id: str
            id of the pool you want to run the job on.
        """
        pool_info = batch.models.PoolInformation(pool_id=pool_id)
        new_job = batch.models.JobAddParameter(
            id=job_id,
            pool_info=pool_info,
            # common_environment_settings=[
            #     batch.models.EnvironmentSetting(
            #         name="xxxxxxx",
            #         value="xxxxxxx",
            #     ),
            # ],
        )
        self.batch_client.job.add(new_job)

    def delete_job(self, job_id: str, force: bool = False) -> None:
        """Deletes a job (incl associated tasks) by a given job id.

        Parameters
        ----------
        job_id: str
            id of the job to be deleted. All associated tasks are deleted as well.
        force: bool
            Whether or not you want to force the deletion, even when a job is running.
        """
        if not force and not self.is_job_completed(job_id):
            raise RuntimeError(
                "ERROR: Job (tasks within job) did not reach 'Completed' state. If you"
                " still want to delete the job, use `force=True`."
            )
        else:
            self.batch_client.job.delete(job_id)

    def create_python_task(
        self,
        job_id: str,
        script: str,
        docker_image: str,
        env_vars: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Creates and adds (and thus executes) a task, using a Python script. The
        script will be executed like this: `python <your-python-script>`. The script
        must therefore be included in the docker image as well.

        Parameters
        ----------
        job_id: str
            Job_id to add the tasks to.
        script: str
            Name of the python script to be executed
        docker_image: str
            Name of the docker image (including tag and available in ACR, e.g.
            myimage:v0.0.7).
        env_vars: Optional[Dict[str, Any]]
            Optional dictionary of key-value pairs to be set as environment variable
            on a task level. Defaults to None.
        """

        now = datetime.datetime.now().isoformat().replace(":", "-").replace(".", "_")
        task_id = f"task_{now}"
        container_settings = batch.models.TaskContainerSettings(
            image_name=f"{self.config['azure']['acr']['server']}/{docker_image}",
            # task gets same working directory as docker image's WORKDIR
            working_directory="containerImageDefault",
            # root login instead of Azure Batch' default user: '_azbatch'
            container_run_options="-u=0",
        )

        batch_env_vars = [
            batch.models.EnvironmentSetting(name=k, value=v)
            for k, v in env_vars.items()
        ]

        new_task = batch.models.TaskAddParameter(
            id=task_id,
            command_line=f"python {script}",
            container_settings=container_settings,
            environment_settings=batch_env_vars,
        )

        self.batch_client.task.add(job_id, new_task)

    def delete_task(
        self,
        job_id: str,
        task_id: str,
        force: bool = False,
    ) -> None:
        """Deletes a task by a given job and task id.

        Parameters
        ----------
        job_id: str
            id of associated job of which you want to delete the task.
        task_id: str
            id of the task you would like to delete.
        force: bool
            Whether or not you want to force the deletion, even when a task is running.
        """
        if not force and not self.is_task_completed(job_id, task_id):
            raise RuntimeError(
                "ERROR: Task did not reach 'Completed' state. If you still want "
                "to delete the task, use `force=True`."
            )
        else:
            self.batch_client.task.delete(job_id, task_id)

    def is_task_completed(self, job_id: str, task_id: str) -> bool:
        """Checks whether a task is completed."""
        task = self.batch_client.task.get(job_id, task_id)
        return task.state != batch.models.TaskState.completed

    def is_job_completed(self, job_id: str) -> bool:
        """Checks whether a job (incl all associated tasks) are completed."""
        tasks = self.batch_client.task.list(job_id)
        complete_tasks = [
            task.state == batch.models.TaskState.completed for task in tasks
        ]

        return all(complete_tasks)

    def wait_for_tasks_to_complete(
        self,
        job_id: str,
        timeout: Optional[datetime.timedelta] = datetime.timedelta(minutes=60),
    ) -> bool:
        """Method that monitors the task(s) for a specific job, and waits until all
        tasks are completed. (state == completed) This doesn't take into account
        whether or not the task/job succeeded.

        Parameters
        ----------
        job_id: str
            job_id for the job to wait for the tasks to complete.
        timeout: datetime.timedelta
            time after which the job should stop. This is to prevent forever spinning
            machines. Defaults to 60 minutes.

        Returns
        -------
        : bool
            returns True when all tasks in the job completed.
            Raises when timeout exceeds.

        """
        timeout_expiration = datetime.datetime.now() + timeout
        logger.info(
            f"Monitoring all tasks for 'Completed' state, timeout in {str(timeout)}..."
        )

        while datetime.datetime.now() < timeout_expiration:
            if self.is_job_completed(job_id):
                return True
            else:
                time.sleep(5)

        raise RuntimeError(
            "ERROR: Tasks did not reach 'Completed' state within "
            f"timeout period of {str(timeout)}"
        )
