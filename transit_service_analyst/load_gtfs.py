from .gtfs_service import Service_Utils


def load_gtfs(gtfs_dir, service_date):
    gtfs_service = Service_Utils(gtfs_dir, service_date)
    return gtfs_service
