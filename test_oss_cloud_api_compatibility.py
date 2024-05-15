import json
import re

import pytest


@pytest.fixture
def oss_schema():
    return load_schema("oss_schema.json")


@pytest.fixture
def cloud_schema():
    return load_schema("cloud_schema.json")


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


OSS_PATH_IGNORE_REGEXES = {
    # CSRF protection is OSS only.
    re.compile(r"^/api/csrf-token$"),
    # avoid experimental routes to allow for fast iterations
    re.compile(r".*experimental.*"),
}

# OSS has support for some request properties that are not yet in Cloud, but
# that are forward compatible.
FORWARD_COMPATIBLE_OSS_REQUEST_PROPS = {
    "/api/deployments/": ["job_variables"],
    "/api/deployments/{id}": ["job_variables"],
}

# OSS has support for some properties in its API types that are not yet in
# Cloud but that are forward compatible.
FORWARD_COMPATIBLE_OSS_API_TYPE_PROPS = {
    "DeploymentCreate": ["job_variables"],
    "DeploymentUpdate": ["job_variables"],
    "DeploymentResponse": ["job_variables"],
}


def generate_oss_paths_by_method():
    oss_paths: dict[str, dict[str, dict]] = load_schema("oss_schema.json", key="paths")
    output = []
    for endpoint, path in oss_paths.items():
        if any(regex.match(endpoint) for regex in OSS_PATH_IGNORE_REGEXES):
            continue
        for method in path.keys():
            output.append((method, endpoint, path))
    return output


def generate_oss_types():
    oss_types = load_schema("oss_schema.json", key="components")["schemas"]
    output = []
    for name, typ in oss_types.items():
        output.append((name, typ))
    return output


def lookup_schema_ref(schema, ref):
    if not ref:
        return

    keys = ref.split("/")
    for key in keys:
        if key == "#":
            continue
        schema = schema[key]
    return schema


def convert_oss_endpoint_to_cloud(endpoint):
    # Collections endpoint is not nested under accounts and workspaces in Cloud
    if endpoint == "/api/collections/views/{view}":
        return endpoint
    endpoint = endpoint.replace(
        "api", "api/accounts/{account_id}/workspaces/{workspace_id}"
    )
    return endpoint


OSS_PATHS = generate_oss_paths_by_method()
OSS_TYPES = generate_oss_types()


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

    def param_type_and_format(schema):
        if "anyOf" in schema:
            return [(item["type"], item.get("format")) for item in schema["anyOf"]]
        else:
            return (schema.get("type"), schema.get("format"))

    # check schemas
    cloud_params = {
        p["name"]: (
            p["in"],
            p["required"],
            *param_type_and_format(p["schema"]),
        )
        for p in cloud_params
    }

    oss_params = {
        p["name"]: (
            p["in"],
            p["required"],
            *param_type_and_format(p["schema"]),
        )
        for p in oss_params
    }

    # Some sets of endpoints do not require x-prefect-api-version header in Cloud
    # because they are part of non-orchestration services
    ENDPOINT_GROUPS_WITHOUT_API_VERSION = [
        "collections",
        "events",
        "automations",
        "templates",
    ]

    if any(group in cloud_endpoint for group in ENDPOINT_GROUPS_WITHOUT_API_VERSION):
        oss_params.pop("x-prefect-api-version", None)

    assert cloud_params == oss_params


@pytest.mark.parametrize(
    "oss_path",
    OSS_PATHS,
    ids=[f"{method.upper()}: {endpoint}" for (method, endpoint, _) in OSS_PATHS],
)
def test_api_request_bodies_are_compatible(oss_path, oss_schema, cloud_schema):
    "Note: this test does not test sorts or filters yet."
    cloud_paths = cloud_schema["paths"]

    method, endpoint, path = oss_path
    cloud_endpoint = convert_oss_endpoint_to_cloud(endpoint)

    if cloud_endpoint not in cloud_paths:
        return  # path existence is checked in another test

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
    oss_ref_schema = lookup_schema_ref(schema=oss_schema, ref=oss_body_schema) or dict(
        type=None, properties={}
    )

    def hashable_default(d):
        # Some default values are lists or other unhashable types, so convert
        # them to a string representation for comparison purposes.
        default = d.get("default")
        if default == []:
            return "list"
        elif default == {}:
            return "dict"
        else:
            return default

    def extract_types(d):
        if "type" in d:
            return {d["type"]}
        elif "anyOf" in d:
            return {item.get("type") for item in d["anyOf"] if item.get("type")}
        return set()

    # TODO: add sorts and filters
    prop_gettr = lambda name, d: (
        name,
        extract_types(d),
        d.get("format"),
        hashable_default(d),
        d.get("deprecated"),
    )

    cloud_props = (
        cloud_ref_schema["type"],
        {name: prop_gettr(name, d) for name, d in cloud_ref_schema["properties"].items()},
    )
    oss_props = (
        oss_ref_schema["type"],
        {
            name: prop_gettr(name, d)
            for name, d in oss_ref_schema["properties"].items()
            if name not in FORWARD_COMPATIBLE_OSS_REQUEST_PROPS.get(endpoint, [])
        },
    )

    ## have to do some delicate handling here - request bodies are compatible so long as:
    ## - OSS fields are always present in Cloud
    ## - new Cloud fields aren't required (this is difficult to check right now as it's method dependent!)
    assert cloud_props[0] == oss_props[0]

    # ensure every OSS field is present in Cloud
    # ensure the properties are the same or a subset (like in the case of type)
    for field_name, (oss_name, oss_types, oss_format, oss_default, oss_deprecated) in oss_props[1].items():
        assert field_name in cloud_props[1]
        (cloud_name, cloud_types, cloud_format, cloud_default, cloud_deprecated) = cloud_props[1][field_name]

        assert oss_name == cloud_name
        assert oss_types <= cloud_types
        assert oss_format == cloud_format
        assert oss_default == cloud_default
        assert oss_deprecated == cloud_deprecated


@pytest.mark.parametrize("oss_type", OSS_TYPES, ids=[name for (name, _) in OSS_TYPES])
def test_oss_api_types_are_cloud_compatible(oss_type, cloud_schema):
    cloud_types = cloud_schema["components"]["schemas"]
    name, typ = oss_type

    # ignore missing for now, as there are name incompatibilities to study
    if name not in cloud_types:
        return

    for master_key in ["properties", "required", "enum", "type"]:
        oss_props, cloud_props = (
            typ.get(master_key, {}),
            cloud_types[name].get(master_key, {}),
        )

        if not isinstance(oss_props, dict):
            if isinstance(oss_props, list):
                # OSS types should be a subset of Cloud
                assert set(oss_props) <= set(cloud_props)
            else:
                assert oss_props == cloud_props

            return

        items = [
            (k, v)
            for k, v in oss_props.items()
            if k not in FORWARD_COMPATIBLE_OSS_API_TYPE_PROPS.get(name, [])
        ]

        for field_name, props in items:
            assert field_name in cloud_props

            oss_options = set()
            cloud_options = set()

            # types can be specified in either the `type` field
            # for a single value or the `anyOf` field for multiple values

            if props.get("type"):
                oss_options = {props.get("type")}
            elif props.get("anyOf"):
                oss_options = {opt.get("type") for opt in props.get("anyOf") if opt.get("type")}

            if cloud_props[field_name].get("type"):
                cloud_options = {cloud_props[field_name].get("type")}
            elif cloud_props[field_name].get("anyOf"):
                cloud_options = {opt.get("type") for opt in cloud_props[field_name].get("anyOf") if opt.get("type")}

            assert oss_options <= cloud_options


def deep_tuple(o):
    """Given an object `o` that is either a dictionary, a list, or any other hashable
    type, return a tuple representation of it where all components are also converted
    into tuples."""
    if isinstance(o, dict):
        return tuple((k, deep_tuple(v)) for k, v in o.items())
    elif isinstance(o, list):
        return tuple(deep_tuple(v) for v in o)
    else:
        return o
