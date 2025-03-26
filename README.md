## arches-querysets

A Django-native interface for expressing application logic,
querying business data, or building APIs using semantic labels: node and nodegroup aliases (rather than UUIDs).

### Installation
(The optional integration with Django REST Framework is included below.)

In pyproject.toml:
```
dependencies = [
    ...
    "arches_querysets @ git+https://github.com/archesproject/arches-querysets@main",
    "djangorestframework",
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

REST_FRAMEWORK = {  # if you are using the Django REST Framework integration
    # TODO: choose most appropriate default.
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly"
    ],
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.LimitOffsetPagination",
    "PAGE_SIZE": API_MAX_PAGE_SIZE,
}
```

### Usage
```
forthcoming
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
    - Designed to wrap other backends besides Postgres, e.g. SpatiaLite


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
        - admin
- Reduce drift against core Arches development: validation traffic still routed through core arches
- Fully dynamic:
    - does not require declaring "well-known" models
    - does not require database migrations
    - does not require an additional database adapter layer


### Project status, roadmap

As the API stabilizes, elements may be proposed for inclusion in archesproject/arches as ready.

The initial goal is for the first release to support both Arches 7.6 and 8.0.

### Contributing

Contributions and bug reports are welcome!

### Thanks

We are grateful to members of the Arches community that have shared prior work in this area: in particular, the approaches linked in the [precedents](#how-does-this-compare-to-other-approaches).
