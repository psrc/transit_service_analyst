from .gtfs_service import Service_Utils

def load_gtfs(gtfs_dir, service_date, start_time, end_time, route_representation='route_id'):
    gtfs_service = Service_Utils(gtfs_dir, service_date, start_time, end_time)
    return gtfs_service
