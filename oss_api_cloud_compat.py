import json


def load_schema(fpath: str, key: str = None):
    with open(fpath, 'r') as f:
        schema = json.load(f)
    if key:
        return schema[key]
    else:
        return schema


def extra_oss_paths(cloud_paths, oss_paths):
    errors = []
    for endpoint, path in oss_paths.items():
        cloud_endpoint = endpoint.replace("api", "api/accounts/{account_id}/workspaces/{workspace_id}")
        for method in path.keys():
            if cloud_endpoint not in cloud_paths:
                if "Admin" not in path[method]['tags']:
                    errors.append(f"{method.upper()}: {cloud_endpoint}")
    return errors


def test_oss_api_spelling_is_cloud_compatible():
    cloud_paths = load_schema('cloud_schema.json', key='paths')
    oss_paths = load_schema('oss_schema.json', key='paths')
    errors = extra_oss_paths(cloud_paths, oss_paths)
    list_of_routes = "\n".join(errors)
    error_msg = f"The following API routes were present in OSS but not in Cloud: {list_of_routes}"
    assert not errors, error_msg


if __name__ == "__main__":
    test_oss_api_is_cloud_compatible()
