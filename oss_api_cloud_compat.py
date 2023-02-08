import json


def load_schema(fpath: str, key: str = None):
    with open(fpath, 'r') as f:
        schema = json.load(f)
    if key:
        return schema[key]
    else:
        return schema


def collect_extra_oss_paths(cloud_paths, oss_paths):
    errors = []
    for endpoint, path in oss_paths.items():
        cloud_endpoint = endpoint.replace("api", "api/accounts/{account_id}/workspaces/{workspace_id}")
        for method in path.keys():
            if cloud_endpoint not in cloud_paths:
                if "Admin" not in path[method]['tags']:
                    errors.append(f"{method.upper()}: {cloud_endpoint}")
    return errors


def check_type_incompatibility(cloud_types, oss_types):
    missing, type_issues = [], []

    for name, typ in oss_types.items():
        if name not in cloud_types:
            missing.append(name)
            continue

        for master_key in ['properties', 'required', 'enum', 'type']:
            oss_props, cloud_props = typ.get(master_key, {}), cloud_types[name].get(master_key, {})

            if not isinstance(oss_props, dict):
                if oss_props != cloud_props:
                    type_issues.append(f"{name}.{master_key}")
                continue

            for field_name, props in oss_props.items():
                print(f"{name}.{field_name}")
                if field_name not in cloud_props:
                    type_issues.append(f"{name}.{field_name}")
                    continue
                if props.get('type') != cloud_props[field_name].get('type'):
                    type_issues.append(f"{name}.{field_name}")
    return missing, type_issues


def test_oss_api_spelling_is_cloud_compatible():
    cloud_paths = load_schema('cloud_schema.json', key='paths')
    oss_paths = load_schema('oss_schema.json', key='paths')
    errors = collect_extra_oss_paths(cloud_paths, oss_paths)
    list_of_routes = "\n".join(errors)
    error_msg = f"The following API routes were present in OSS but not in Cloud: {list_of_routes}"
    assert not errors, error_msg


def test_oss_api_types_are_cloud_compatible():
    cloud_types = load_schema('cloud_schema.json', key='components')
    oss_types = load_schema('oss_schema.json', key='components')
    missing, type_issues = check_type_incompatibility(cloud_types['schemas'], oss_types['schemas'])

    # ignore missing for now, as there are name incompatibilies to study
    list_of_missing = "\n".join(missing)
    error_msg = ""
    if missing:
        # error_msg += f"The following API types were present in OSS but not Cloud: {list_of_missing}"
        pass

    if type_issues:
        list_of_issues = "\n".join(type_issues)
        error_msg += f"The following API types have incompatible fields between OSS and Cloud: {list_of_issues}"
    assert not type_issues, error_msg


if __name__ == "__main__":
    test_oss_api_spelling_is_cloud_compatible()
    test_oss_api_types_are_cloud_compatible()
