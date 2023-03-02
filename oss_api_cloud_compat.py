import argparse
import json
import pytest
import sys


@pytest.fixture
def oss_schema():
    return "oss_schema.json"


@pytest.fixture
def cloud_schema():
    return "cloud_schema.json"


@pytest.fixture
def cloud_paths():
    return load_schema("cloud_schema.json", key="paths")


def load_schema(fpath: str, key: str = None):
    with open(fpath, "r") as f:
        schema = json.load(f)
    if key:
        return schema[key]
    else:
        return schema


def generate_oss_paths_by_method():
    oss_paths = load_schema("oss_schema.json", key="paths")
    output = []
    for endpoint, path in oss_paths.items():
        for method in path.keys():
            output.append((method, endpoint, path))
    return output


def convert_oss_endpoint_to_cloud(endpoint):
    endpoint = endpoint.replace(
        "api", "api/accounts/{account_id}/workspaces/{workspace_id}"
    )
    return endpoint


OSS_PATHS = generate_oss_paths_by_method()


@pytest.mark.parametrize(
    "oss_path",
    OSS_PATHS,
    ids=[f"{method.upper()}: {endpoint}" for (method, endpoint, _) in OSS_PATHS],
)
def test_oss_api_spelling_is_cloud_compatible(oss_path, cloud_paths):
    # error_msg = f"The following API routes were present in OSS but not in Cloud: \n{list_of_routes}"
    method, endpoint, path = oss_path
    cloud_endpoint = convert_oss_endpoint_to_cloud(endpoint)
    if not any(
        tag in ["Admin", "Flow Run Notification Policies", "Root"]
        for tag in path[method]["tags"]
    ):
        assert cloud_endpoint in cloud_paths, f"{method.upper()}: {cloud_endpoint}"


@pytest.mark.parametrize(
    "oss_path",
    OSS_PATHS,
    ids=[f"{method.upper()}: {endpoint}" for (method, endpoint, _) in OSS_PATHS],
)
def test_api_path_parameters_are_compatible(oss_path, cloud_paths):
    method, endpoint, path = oss_path
    cloud_endpoint = convert_oss_endpoint_to_cloud(endpoint)
    if cloud_endpoint not in cloud_paths:
        return  # path existence is checked in another test

    cloud_params = cloud_paths[cloud_endpoint][method].get("parameters", [])
    cloud_params = [
        p
        for p in cloud_params
        if p["name"] not in ("account_id", "workspace_id", "token_cost")
    ]
    oss_params = path[method].get("parameters", [])

    # check schemas
    cloud_params = {
        p["name"]: (
            p["in"],
            p["required"],
            p["schema"]["type"],
            p["schema"].get("format"),
        )
        for p in cloud_params
    }
    oss_params = {
        p["name"]: (
            p["in"],
            p["required"],
            p["schema"]["type"],
            p["schema"].get("format"),
        )
        for p in oss_params
    }

    assert cloud_params == oss_params


def lookup_schema_ref(schema, ref):
    if not ref:
        return

    keys = ref.split("/")
    for key in keys:
        if key == "#":
            continue
        schema = schema[key]
    return schema


def check_body_compatibility(cloud_schema, oss_schema):
    cloud_paths = cloud_schema["paths"]
    oss_paths = oss_schema["paths"]

    errors = []
    for endpoint, path in oss_paths.items():
        cloud_endpoint = convert_oss_endpoint_to_cloud(endpoint)
        if cloud_endpoint not in cloud_paths:
            continue  # path existence is checked in another test
        for method in path.keys():
            # easier to use safe gets than handle all possible ways they could differ
            cloud_body = cloud_paths[cloud_endpoint][method].get("requestBody", {})
            oss_body = path[method].get("requestBody", {})

            cloud_body_schema = (
                cloud_body.get("content", {})
                .get("application/json", {})
                .get("schema", {})
                .get("$ref")
            )
            oss_body_schema = (
                oss_body.get("content", {})
                .get("application/json", {})
                .get("schema", {})
                .get("$ref")
            )

            cloud_ref_schema = lookup_schema_ref(
                schema=cloud_schema, ref=cloud_body_schema
            ) or dict(type=None, properties={})
            oss_ref_schema = lookup_schema_ref(
                schema=oss_schema, ref=oss_body_schema
            ) or dict(type=None, properties={})

            # TODO: add sorts and filters
            prop_gettr = lambda name, d: (
                name,
                d.get("type"),
                d.get("format"),
                d.get("default"),
                d.get("deprecated"),
            )

            cloud_props = (
                cloud_ref_schema["type"],
                {
                    prop_gettr(name, d)
                    for name, d in cloud_ref_schema["properties"].items()
                },
            )
            oss_props = (
                oss_ref_schema["type"],
                {
                    prop_gettr(name, d)
                    for name, d in oss_ref_schema["properties"].items()
                },
            )

            if cloud_props != oss_props:
                errors.append(
                    f"{method.upper()}: {cloud_endpoint} has body incompatibilities:\nCLOUD BODY SCHEMA:\n{cloud_props}\nOSS BODY SCHEMA:\n{oss_props}"
                )

    return errors


def check_type_incompatibility(cloud_types, oss_types):
    missing, type_issues = [], []

    for name, typ in oss_types.items():
        if name not in cloud_types:
            missing.append(name)
            continue

        for master_key in ["properties", "required", "enum", "type"]:
            oss_props, cloud_props = typ.get(master_key, {}), cloud_types[name].get(
                master_key, {}
            )

            if not isinstance(oss_props, dict):
                if oss_props != cloud_props:
                    type_issues.append(f"{name}.{master_key}")
                continue

            for field_name, props in oss_props.items():
                if field_name not in cloud_props:
                    type_issues.append(f"{name}.{field_name}")
                    continue
                if props.get("type") != cloud_props[field_name].get("type"):
                    type_issues.append(f"{name}.{field_name}")
    return missing, type_issues


def test_api_request_bodies_are_compatible(oss_schema, cloud_schema):
    "Note: this test does not test sorts or filters yet."
    cloud_paths = load_schema(cloud_schema)
    oss_paths = load_schema(oss_schema)
    errors = check_body_compatibility(cloud_paths, oss_paths)
    list_of_issues = "\n".join(errors)
    error_msg = f"The following API endpoints have incompatible request bodies: \n{list_of_issues}"
    assert not errors, error_msg


def test_oss_api_types_are_cloud_compatible(oss_schema, cloud_schema):
    cloud_types = load_schema(cloud_schema, key="components")
    oss_types = load_schema(oss_schema, key="components")
    missing, type_issues = check_type_incompatibility(
        cloud_types["schemas"], oss_types["schemas"]
    )

    # ignore missing for now, as there are name incompatibilies to study
    list_of_missing = "\n".join(missing)
    error_msg = ""
    if missing:
        # error_msg += f"The following API types were present in OSS but not Cloud: {list_of_missing}"
        pass

    if type_issues:
        list_of_issues = "\n".join(type_issues)
        error_msg += f"The following API types have incompatible fields between OSS and Cloud: \n{list_of_issues}"
    assert not type_issues, error_msg
