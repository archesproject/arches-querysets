## arches-querysets

A Django-native interface for Arches implementers to express application logic,
query business data, or build APIs using node and nodegroup aliases.

Please see the [project page](http://archesproject.org/) for more information on the Arches project.

### Installation
The optional API integration with Django REST Framework is included below.

`pip install 'arches-querysets[drf]'`, or, in pyproject.toml:
```
dependencies = [
    ...
    "arches_querysets[drf]",
]
```
In settings.py:
```
INSTALLED_APPS = [
    ...
    "arches_querysets",
    "rest_framework",  # if you are using the Django REST Framework integration
    ...
]
```
In urls.py:
```
from arches_querysets.urls import arches_rest_framework_urls
urlpatterns = [
    ...
    *arches_rest_framework_urls,
]
```

For developer install instructions, see the [Developer Setup](#developer-setup-for-contributing-to-the-arches-querysets-project) section below.

### Quickstart
```shell
python manage.py add_test_data
python manage.py runserver
```

#### API usage
Log in to Arches, then visit `/api/resources/datatype_lookups` to explore the data for the ["datatype_lookups" test model](https://github.com/archesproject/arches-querysets/blob/88c284a458fbf2f4757621d0d381a9bf4ea6a96c/tests/utils.py#L24) using the [browsable API](https://www.django-rest-framework.org/topics/browsable-api/).

You'll see a tree of tiles with nodegroup data grouped under an `"aliased_data"` key:

<details>
<summary>Example response</summary>

Note that below "string", "number", "concept", etc. are node aliases.
```json
GET /api/resource/datatype_lookups

HTTP 200 OK
Allow: GET, POST, HEAD, OPTIONS
Content-Type: application/json
Vary: Accept

{
    "count": 2,
    "next": null,
    "previous": null,
    "results": [
        {
            "resourceinstanceid": "486412dd-fed8-45b4-a0e0-5cd738f0eaeb",
            "aliased_data": {
                "datatypes_1": {
                    "tileid": "8368dbc8-7cbc-4080-8623-46c928f011b8",
                    "resourceinstance": "486412dd-fed8-45b4-a0e0-5cd738f0eaeb",
                    "nodegroup": "c8d6ae9b-d9e4-4ecc-87ef-6183d84df305",
                    "parenttile": null,
                    "aliased_data": {
                        "string": {
                            "display_value": "forty-two",
                            "interchange_value": {
                                "en": {
                                    "value": "forty-two",
                                    "direction": "ltr"
                                }
                            }
                        },
                        "number": {
                            "display_value": "42",
                            "interchange_value": 42
                        },
                        "concept": {
                            "display_value": "Arches",
                            "interchange_value": {
                                "concept_id": "00000000-0000-0000-0000-000000000001",
                                "language_id": "en",
                                "value": "Arches",
                                "valueid": "d8c60bf4-e786-11e6-905a-b756ec83dad5",
                                "valuetype_id": "prefLabel"
                            }
                        },
                        ...
```
</details>

- At the bottom of the page, switch to the JSON view to edit the payload and save back. (You can also provide interchange
values directly rather than wrapping them under an `interchange_value` key.)
- The schema for the interchange value is [tested here](https://github.com/archesproject/arches-querysets/blob/main/tests/test_datatypes.py).
- Inherit from the [generic views](https://github.com/archesproject/arches-querysets/blob/main/arches_querysets/rest_framework/generic_views.py) when composing your own routes to customize pagination, permissions, validation etc.

#### Direct QuerySet usage
This graph has nodes with aliases for each datatype, e.g. "string", with node values all referencing the number 42 in some way:

```py
In [1]: from pprint import pprint

In [2]: objects = ResourceTileTree.get_tiles(graph_slug="datatype_lookups")

In [3]: for result in objects.filter(string__any_lang_contains='two'):
    ...:     pprint(result)
    ...:     pprint(result.aliased_data.datatypes_1.aliased_data)

<ResourceTileTree: Datatype Lookups: Resource referencing 42 (486412dd-fed8-45b4-a0e0-5cd738f0eaeb)>
AliasedData(string={'en': {'direction': 'ltr', 'value': 'forty-two'}},
            number=42,
            concept=<Value: Value object (d8c60bf4-e786-11e6-905a-b756ec83dad5)>,
            concept_list=[<Value: Value object (d8c60bf4-e786-11e6-905a-b756ec83dad5)>],
            date='2042-04-02',
            edtf=None,
            annotation=None,
            url={'url': 'http://www.42.com/', 'url_label': '42.com'},
            resource_instance=<ResourceInstance: Datatype Lookups: Resource referencing 42 (486412dd-fed8-45b4-a0e0-5cd738f0eaeb)>,
            resource_instance_list=[<ResourceInstance: Datatype Lookups: Resource referencing 42 (486412dd-fed8-45b4-a0e0-5cd738f0eaeb)>],
            boolean=True,
            domain_value=None,
            domain_value_list=None,
            non_localized_string='forty-two',
            geojson_feature_collection=None,
            file_list=[{'accepted': True,
                        'altText': {'en': {'direction': 'ltr',
                                           'value': 'Illustration of recent '
                                                    'accessibility '
                                                    'improvements'}},
                        'attribution': {'en': {'direction': 'ltr',
                                               'value': 'Arches'}},
                        'content': 'blob:http://localhost:8000/8cf874b3-d84d-4e45-bd32-419dc2fcedeb',
                        'description': {'en': {'direction': 'ltr',
                                               'value': 'Recent versions of '
                                                        'arches have 42 '
                                                        'improved '
                                                        'accessibility '
                                                        'characteristics.'}},
                        'file_id': '11e5c7d2-6e31-4a7a-af74-25e6064ab40c',
                        'height': 2042,
                        'index': 0,
                        'lastModified': 1723503486969,
                        'name': '42_accessibility_improvements.png',
                        'size': 2042,
                        'status': 'added',
                        'title': {'en': {'direction': 'ltr',
                                         'value': '42 Accessibility '
                                                  'Improvements'}},
                        'type': 'image/png',
                        'url': 'http://www.archesproject.org/blog/static/42.png',
                        'width': 2042}],
            node_value='8368dbc8-7cbc-4080-8623-46c928f011b8')
{'en': {'direction': 'ltr', 'value': 'forty-two'}}

In [3]: result.aliased_data.datatypes_1.aliased_data.string = 'new value'

In [4]: result.save()
```

### How would this help an Arches developer?

If you wish to stand up an API to power a frontend, rediscovering patterns for routes, views, filtering, validation, pagination, and error handling in every project can increase maintenance burdens and prevent developers with relatively less Arches experience from making productive contributions. Given the numerous translations necessary among resources, nodes, and tiles, expressing queries in a readable way using the Django ORM can be quite difficult--making it tempting to drop to raw SQL, which comes with its own security, reusability, and caching drawbacks. Finally, having to reference node values by UUIDs is a developer experience negative.

Pushing tile transforms out of projects and into a generic application with test coverage reduces the surface area for errors or test coverage gaps in projects. 

### How does this compare to other approaches?

Other Arches community members have developed parallel solutions to related use cases.
In brief:

- archesproject/arches: [Resource Report API](https://github.com/archesproject/arches/blob/4b5e67c910aa3fac2538d0ae31e904242b3c1ccb/arches/urls.py#L607-L621) powered by "label-based graph":
    - maps tile data by semantic labels
    - supports retrieve only
    - limited support for filtering, language selection (e.g. hide empty nodes)
- archesproject/arches: [Relational Views](https://arches.readthedocs.io/en/stable/developing/reference/import-export/#sql-import):
    - SQL-based approach for ETL, supports full CRUD (create/retrieve/update/delete) cycle
    - Can be linked to python models via `managed=False` Django models
    - Skips all python-level validation logic
    - Requires direct database operations (migrations) to create views
    - Some known performance overhead
    - Unknown status of custom/future datatypes
- [flaxandteal/arches-orm](https://flaxandteal.github.io/arches-orm/docs/quickstart/)
    - Server-side access to pythonic resource models after fetching them from the database
    - Unified abstraction layer for resources, whether from Django, Arches APIs or JSON exports


Factors differentiating the arches-querysets approach include:

-   Expressing create/retrieve/update/delete operations (and filtering) using Django QuerySets:
    - interoperability with other Django tools and third-party packages:
        - [Django REST Framework](https://www.django-rest-framework.org/)
        - [DRF Spectacular](https://drf-spectacular.readthedocs.io/) (schema generation)
        - [speculative:] Django GraphQL API clients?
        - [django-filter](https://django-filter.readthedocs.io/)
        - [django-debug-toolbar](https://django-debug-toolbar.readthedocs.io/)
        - etc.
    - familiar interface for developers exposed to Django
    - can leverage built-in features of QuerySets:
        - chainable
        - lazy
        - cached
        - fine-grained control over related object fetching (to address so-called "N+1 queries" performance issues)
        - overridable
    - can leverage other built-in Django features:
        - pagination
        - migrations
        - registering custom SQL lookups
- Reduce drift against core Arches development: validation traffic still routed through core arches
- Fully dynamic:
    - does not require declaring "well-known" models
    - does not require database migrations
    - does not require an additional database adapter layer


### Project status, roadmap

As the API stabilizes, elements may be proposed for inclusion in archesproject/arches as ready.

The first version supports both Arches 7.6 and 8.0.

### Contributing

Contributions and bug reports are welcome!

### Thanks

We are grateful to members of the Arches community that have shared prior work in this area: in particular, the approaches linked in the [precedents](#how-does-this-compare-to-other-approaches).

### Developer Setup (for contributing to the Arches Querysets project)

1. Download the arches-querysets repo:

    a.  If using the [Github CLI](https://cli.github.com/): `gh repo clone archesproject/arches-querysets`
    
    b.  If not using the Github CLI: `git clone https://github.com/archesproject/arches-querysets.git`

2. Download the arches package:

    a.  If using the [Github CLI](https://cli.github.com/): `gh repo clone archesproject/arches`

    b.  If not using the Github CLI: `git clone https://github.com/archesproject/arches.git`

3. Create a virtual environment outside of both repositories: 
    ```
    python3 -m venv ENV
    ```

4. Activate the virtual enviroment in your terminal:
    ```
    source ENV/bin/activate
    ```

5. Navigate to the `arches-querysets` directory, and install the project (with optional and development dependencies):
    ```
    cd arches-querysets
    pip install -e '.[drf]' --group dev
    ```

6. Also install core arches for local development:
    ```
    pip install -e ../arches
    ```

7. Run the Django server:
    ```
    python manage.py runserver
    ```

## Committing changes

NOTE: Changes are committed to the arches-querysets repository. 

1. Navigate to the repository
    ```
    cd arches-querysets
    ```

2. Cut a new git branch
    ```
    git checkout origin/main -b my-descriptive-branch-name
    ```

3. Add your changes to the current git commit
    ```
    git status
    git add -- path/to/file path/to/second/file
    git commit -m "Descriptive commit message"
    ```

4. Update the remote repository with your commits:
    ```
    git push origin HEAD
    ```

5. Navigate to https://github.com/archesproject/arches-querysets/pulls to see and commit the pull request.
