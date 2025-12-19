from arches.app.models.resource import Resource
from celery import shared_task


@shared_task
def index_resource(resource_id):
    proxy_resource = (
        Resource.objects.filter(pk=resource_id)
        # .select_related("graph__publication")
        .get()
    )
    proxy_resource.index()
