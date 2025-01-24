import kopf
import logging
import kubernetes

logger = logging.getLogger(__name__)


@kopf.on.startup()
def startup_fn(logger, **kwargs):
    logger.info("Tuskr controller is starting up.")
    kubernetes.config.load_incluster_config()  # or load_kube_config() if testing locally


#
# Handler for JobTemplate (much like a CronJob)
#
@kopf.on.create("tuskr.io", "v1alpha1", "jobtemplates")
def create_jobtemplate(body, spec, **kwargs):
    """
    This function is called when a new JobTemplate object is created.
    Insert logic here to handle or store the template specifications.
    """
    name = body.get("metadata", {}).get("name")
    logger.info(f"JobTemplate {name} was created with spec: {spec}")
    # Implementation for your job template creation
    # e.g., store a template or do some validations
    return {"message": f"Created JobTemplate {name}"}


@kopf.on.update("tuskr.io", "v1alpha1", "jobtemplates")
def update_jobtemplate(body, spec, **kwargs):
    """
    This function is called when an existing JobTemplate object is updated.
    """
    name = body.get("metadata", {}).get("name")
    logger.info(f"JobTemplate {name} was updated with spec: {spec}")
    # Update logic here
    return {"message": f"Updated JobTemplate {name}"}


@kopf.on.delete("tuskr.io", "v1alpha1", "jobtemplates")
def delete_jobtemplate(body, spec, **kwargs):
    """
    Cleanup or finalization logic when a JobTemplate is deleted.
    """
    name = body.get("metadata", {}).get("name")
    logger.info(f"JobTemplate {name} was deleted.")
    # Cleanup logic here
    return {"message": f"Deleted JobTemplate {name}"}


#
# Handler for Task
#
@kopf.on.create("tuskr.io", "v1alpha1", "tasks")
def create_task(body, spec, **kwargs):
    """
    This function is called when a new Task object is created.
    Based on its specification, you could launch a Job or
    some other resource in the cluster.
    """
    name = body.get("metadata", {}).get("name")
    logger.info(f"Task {name} was created with spec: {spec}")
    # Example: Creating a Kubernetes job, or any other desired resource
    # ...
    return {"message": f"Created Task {name} and launched job."}


@kopf.on.update("tuskr.io", "v1alpha1", "tasks")
def update_task(body, spec, **kwargs):
    """
    Update logic for Task CRD.
    """
    name = body.get("metadata", {}).get("name")
    logger.info(f"Task {name} was updated with spec: {spec}")
    # ...
    return {"message": f"Updated Task {name}"}


@kopf.on.delete("tuskr.io", "v1alpha1", "tasks")
def delete_task(body, spec, **kwargs):
    """
    Cleanup logic for Task CRD.
    """
    name = body.get("metadata", {}).get("name")
    logger.info(f"Task {name} was deleted.")
    # ...
    return {"message": f"Deleted Task {name}"}
