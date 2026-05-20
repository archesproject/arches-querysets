class NodeValueMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_representation(self, value):
        return super().to_representation(value)

    def to_internal_value(self, data):
        # DRF's DateField doesn't handle None despite a few
        # close-but-no-cigar bug reports like:
        # https://github.com/encode/django-rest-framework/issues/4835
        if data is None:
            return None
        return super().to_internal_value(data)
