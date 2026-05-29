import json
import re
from typing import Any

import pytest


PREFECT_V2 = False


@pytest.fixture
def oss_schema():
    schema = load_schema("oss_schema.json")
    if schema["info"]["version"].startswith("2"):
        global PREFECT_V2
        PREFECT_V2 = True
    return schema


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
    re.compile(r"^(/api)?/csrf-token$"),
    # avoid experimental routes to allow for fast iterations
    re.compile(r".*experimental.*"),
    # ignore the UI routes because OSS has it's own distinct UI
    re.compile(r"^(/api)?/ui.*"),
    # task_run_states API permanently returns 410 Gone in Cloud (CLOUD-3414)
    re.compile(r"^(/api)?/task_run_states.*"),
}

# OSS has support for some request properties that are not yet in Cloud, but
# that are forward compatible.
FORWARD_COMPATIBLE_OSS_REQUEST_PROPS = {
    "/deployments/": ["job_variables"],
    "/deployments/{id}": ["job_variables"],
}

# OSS has support for some properties in its API types that are not yet in
# Cloud but that are forward compatible.
FORWARD_COMPATIBLE_OSS_API_TYPE_PROPS = {
    "DeploymentCreate": ["job_variables"],
    "DeploymentUpdate": ["job_variables"],
    "DeploymentResponse": ["job_variables"],
    # ValidationError schema drift: Cloud pins FastAPI 0.121.0 + pydantic 2.12.2,
    # while OSS in this suite is generated from prefect@main (currently FastAPI
    # 0.128.x + pydantic 2.12.5). The newer OSS stack emits extra optional
    # ValidationError fields (ctx/input/url) not present in Cloud's schema.
    "ValidationError": ["ctx", "input", "url"],
}

# Properties for endpoints that are known to be incompatible between OSS and Cloud
# so we want to skip them in the comparison.
# The format is endpoint:method:field:<set of properties to ignore>
# options are: "name", "types", "format", "default", "deprecated"
KNOWN_INCOMPATIBLE_API_REQUEST_PROPS = {
    "/deployments/": {
        "post": {
            "enforce_parameter_schema": {"default"},
        }
    }
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


def normalize_oss_endpoint(endpoint: str) -> str:
    if endpoint.startswith("/api/"):
        return endpoint[4:]
    return endpoint


def convert_oss_endpoint_to_cloud(endpoint: str) -> str:
    normalized_endpoint = normalize_oss_endpoint(endpoint)

    # Collections endpoint is not nested under accounts and workspaces in Cloud
    if normalized_endpoint == "/collections/views/{view}":
        return f"/api{normalized_endpoint}"

    return (
        f"/api/accounts/{{account_id}}/workspaces/{{workspace_id}}"
        f"{normalized_endpoint}"
    )


@pytest.mark.parametrize(
    "oss_endpoint,expected_cloud_endpoint",
    [
        (
            "/api/flow_runs/",
            "/api/accounts/{account_id}/workspaces/{workspace_id}/flow_runs/",
        ),
        (
            "/flow_runs/",
            "/api/accounts/{account_id}/workspaces/{workspace_id}/flow_runs/",
        ),
        (
            "/api/collections/views/{view}",
            "/api/collections/views/{view}",
        ),
        (
            "/collections/views/{view}",
            "/api/collections/views/{view}",
        ),
    ],
)
def test_convert_oss_endpoint_to_cloud_handles_api_prefix_changes(
    oss_endpoint, expected_cloud_endpoint
):
    assert convert_oss_endpoint_to_cloud(oss_endpoint) == expected_cloud_endpoint


def lookup_content_body_schema(body: dict[str, Any]) -> dict[str, Any] | None:
    """Given the schema for an endpoint, find the JSON response's content schema"""
    schema = body.get("content", {}).get("application/json", {}).get("schema", {})

    # In pydantic v1, the schema reference is a single value
    if "$ref" in schema:
        return schema.get("$ref")
    # In pydantic v2, the schema reference is an `allOf` with a single item
    elif "allOf" in schema:
        return schema.get("allOf", [{}])[0].get("$ref")

    return None


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
            # Pydantic v2 renders optional fields with `anyOf` (type, null), but the
            # Pydantic v1 does not, so let's strip all the additional `null` types out
            return [
                (item["type"], item.get("format"))
                for item in schema["anyOf"]
                if item.get("type") != "null"
            ]
        else:
            return [(schema.get("type"), schema.get("format"))]

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
        "logs/download",
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
    normalized_endpoint = normalize_oss_endpoint(endpoint)
    cloud_endpoint = convert_oss_endpoint_to_cloud(endpoint)

    if cloud_endpoint not in cloud_paths:
        return  # path existence is checked in another test

    # easier to use safe gets than handle all possible ways they could differ
    cloud_body = cloud_paths[cloud_endpoint][method].get("requestBody", {})
    oss_body = path[method].get("requestBody", {})

    cloud_body_schema = lookup_content_body_schema(cloud_body)
    oss_body_schema = lookup_content_body_schema(oss_body)

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

    def extract_format(d):
        if "format" in d:
            return d["format"]
        # in practice, this will have only one format
        elif "anyOf" in d:
            for option in d["anyOf"]:
                if option.get("format"):
                    return option.get("format")
        return None

    # TODO: add sorts and filters
    prop_gettr = lambda name, d: (
        name,
        extract_types(d),
        extract_format(d),
        hashable_default(d),
        d.get("deprecated"),
    )

    cloud_props = (
        cloud_ref_schema["type"],
        {
            name: prop_gettr(name, d)
            for name, d in cloud_ref_schema["properties"].items()
        },
    )
    oss_props = (
        oss_ref_schema["type"],
        {
            name: prop_gettr(name, d)
            for name, d in oss_ref_schema["properties"].items()
            if name
            not in FORWARD_COMPATIBLE_OSS_REQUEST_PROPS.get(normalized_endpoint, [])
        },
    )

    # have to do some delicate handling here - request bodies are compatible so long as:
    # - OSS fields are always present in Cloud
    # - new Cloud fields aren't required (this is difficult to check right now as it's method dependent!)
    assert cloud_props[0] == oss_props[0]

    # ensure every OSS field is present in Cloud
    # ensure the property attributes are the same or a subset (like in the case of type)
    for (
        oss_name,
        oss_types,
        oss_format,
        oss_default,
        oss_deprecated,
    ) in oss_props[1].values():
        # Note, this print is here intentionally to make it easier to understand test
        # failures when looping over fields
        print("parameter name:", oss_name)

        if oss_name == "schema" and PREFECT_V2:
            oss_name = "json_schema"  # UI schema validation doesnt really matter for 2.x OSS compat

        assert oss_name in cloud_props[1]
        (
            cloud_name,
            cloud_types,
            cloud_format,
            cloud_default,
            cloud_deprecated,
        ) = cloud_props[1][oss_name]

        # In Pydantic v2, if a field is not required, it's format is not included, so
        # we need to remove it from the comparison
        if "null" in oss_types and oss_format is None and cloud_format is not None:
            cloud_format = None

        # While OSS and Cloud are on different versions of pydantic, there is a
        # discrepancy where any option OSS type (correctly) includes `anyOf` `null`
        # while Cloud does not.
        oss_types.discard("null")

        known_incompatible_props = (
            KNOWN_INCOMPATIBLE_API_REQUEST_PROPS.get(normalized_endpoint, {})
            .get(method, {})
            .get(oss_name, set())
        )

        if "name" not in known_incompatible_props:
            assert oss_name == cloud_name

        if "types" not in known_incompatible_props:
            assert oss_types <= cloud_types

        if "format" not in known_incompatible_props:
            assert oss_format == cloud_format

        if "default" not in known_incompatible_props:
            assert oss_default == cloud_default

        if "deprecated" not in known_incompatible_props:
            assert oss_deprecated == cloud_deprecated


def resolve_response_schema(response_def: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the response schema from an endpoint's response definition."""
    success_response = response_def.get("200", {})
    content = success_response.get("content", {})
    json_content = content.get("application/json", {})
    schema = json_content.get("schema", {})

    if "$ref" in schema:
        return schema["$ref"]
    elif "allOf" in schema:
        return schema.get("allOf", [{}])[0].get("$ref")

    return None


def get_schema_properties_recursive(
    schema: dict[str, Any],
    full_schema: dict[str, Any],
    prefix: str = "",
) -> dict[str, set[str]]:
    """
    Recursively extract all properties from a schema, following $refs.

    Returns a dict mapping property paths to their types.
    For nested objects/arrays, the path is dot-separated (e.g., "results.parameters").
    """
    properties: dict[str, set[str]] = {}

    if "$ref" in schema:
        ref_schema = lookup_schema_ref(full_schema, schema["$ref"])
        if ref_schema:
            return get_schema_properties_recursive(ref_schema, full_schema, prefix)
        return properties

    if "allOf" in schema:
        for item in schema["allOf"]:
            properties.update(
                get_schema_properties_recursive(item, full_schema, prefix)
            )
        return properties

    schema_props = schema.get("properties", {})

    for prop_name, prop_def in schema_props.items():
        full_path = f"{prefix}.{prop_name}" if prefix else prop_name

        prop_types: set[str] = set()
        if "type" in prop_def:
            prop_types.add(prop_def["type"])
        elif "anyOf" in prop_def:
            for option in prop_def["anyOf"]:
                if "type" in option:
                    prop_types.add(option["type"])
                elif "$ref" in option:
                    prop_types.add("$ref")

        properties[full_path] = prop_types

        if prop_def.get("type") == "array":
            items = prop_def.get("items", {})
            if "$ref" in items:
                ref_schema = lookup_schema_ref(full_schema, items["$ref"])
                if ref_schema:
                    nested = get_schema_properties_recursive(
                        ref_schema, full_schema, full_path
                    )
                    properties.update(nested)
            elif items.get("properties"):
                nested = get_schema_properties_recursive(items, full_schema, full_path)
                properties.update(nested)

        elif prop_def.get("type") == "object" and prop_def.get("properties"):
            nested = get_schema_properties_recursive(prop_def, full_schema, full_path)
            properties.update(nested)

        elif "$ref" in prop_def:
            ref_schema = lookup_schema_ref(full_schema, prop_def["$ref"])
            if ref_schema and ref_schema.get("properties"):
                nested = get_schema_properties_recursive(
                    ref_schema, full_schema, full_path
                )
                properties.update(nested)

    return properties


@pytest.mark.parametrize(
    "oss_path",
    OSS_PATHS,
    ids=[f"{method.upper()}: {endpoint}" for (method, endpoint, _) in OSS_PATHS],
)
def test_api_response_bodies_are_compatible(oss_path, oss_schema, cloud_schema):
    """
    Verify that Cloud's response bodies are a superset of OSS's response bodies.

    This test recursively resolves $refs to compare the actual properties returned
    by each endpoint, ensuring that any field present in OSS responses is also
    present in Cloud responses.
    """
    cloud_paths = cloud_schema["paths"]

    method, endpoint, path = oss_path
    cloud_endpoint = convert_oss_endpoint_to_cloud(endpoint)

    if cloud_endpoint not in cloud_paths:
        return  # path existence is checked in another test

    oss_responses = path[method].get("responses", {})
    cloud_responses = cloud_paths[cloud_endpoint][method].get("responses", {})

    oss_response_ref = resolve_response_schema(oss_responses)
    cloud_response_ref = resolve_response_schema(cloud_responses)

    if not oss_response_ref:
        return  # no response schema to compare

    if not cloud_response_ref:
        pytest.fail(
            f"OSS endpoint {endpoint} has a response schema but Cloud endpoint "
            f"{cloud_endpoint} does not"
        )

    oss_response_schema = lookup_schema_ref(oss_schema, oss_response_ref) or {}
    cloud_response_schema = lookup_schema_ref(cloud_schema, cloud_response_ref) or {}

    oss_props = get_schema_properties_recursive(oss_response_schema, oss_schema)
    cloud_props = get_schema_properties_recursive(cloud_response_schema, cloud_schema)

    missing_in_cloud = set(oss_props.keys()) - set(cloud_props.keys())

    if missing_in_cloud:
        pytest.fail(
            f"Cloud response for {method.upper()} {cloud_endpoint} is missing "
            f"properties that OSS has: {sorted(missing_in_cloud)}"
        )


@pytest.mark.parametrize(
    "oss_name_and_type", OSS_TYPES, ids=[name for (name, _) in OSS_TYPES]
)
def test_oss_api_types_are_cloud_compatible(oss_name_and_type, cloud_schema):
    cloud_types = cloud_schema["components"]["schemas"]
    name, oss_type = oss_name_and_type

    # ignore missing for now, as there are name incompatibilities to study
    try:
        cloud_type = cloud_types[name]
    except KeyError:
        return

    # preprocess pydantic v1 schema to match pydantic v2 schema
    def preprocess_pydantic_v1_type(schema):
        # transform any non-required fields to by anyOf (null, type)
        for field_name, props in schema.get("properties", {}).items():
            required_fields = schema.get("required", [])
            if field_name not in required_fields:
                current_definition = schema["properties"][field_name]
                if "anyOf" in current_definition:
                    schema["properties"][field_name]["anyOf"].append({"type": "null"})
                else:
                    schema["properties"][field_name] = {
                        "anyOf": [{"type": "null"}, props]
                    }
                required_fields.append(field_name)
        return schema

    cloud_type = preprocess_pydantic_v1_type(cloud_type)

    for master_key in ["properties", "required", "enum", "type"]:
        oss_props, cloud_props = (
            oss_type.get(master_key, {}),
            cloud_type.get(master_key, {}),
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
            # Note, this print is here intentionally to make it easier to understand
            # test failures when looping over fields
            print("field name:", field_name)

            # fields are ignored in all cases
            if (
                PREFECT_V2
                and name == "StateCreate"
                and field_name in ["timestamp", "id"]
            ):
                continue

            assert field_name in cloud_props

            oss_options = set()
            cloud_options = set()

            # types can be specified in either the `type` field
            # for a single value or the `anyOf` field for multiple values

            if props.get("type"):
                oss_options = {props.get("type")}
            elif props.get("anyOf"):
                oss_options = {
                    opt.get("type") for opt in props.get("anyOf") if opt.get("type")
                }

            if cloud_props[field_name].get("type"):
                cloud_options = {cloud_props[field_name].get("type")}
            elif cloud_props[field_name].get("anyOf"):
                cloud_options = {
                    opt.get("type")
                    for opt in cloud_props[field_name].get("anyOf")
                    if opt.get("type")
                }

            # While OSS and Cloud are on different versions of pydantic, there is a
            # discrepancy where any option OSS type (correctly) includes `anyOf` `null`
            # while Cloud does not.
            oss_options.discard("null")

            assert oss_options <= cloud_options
