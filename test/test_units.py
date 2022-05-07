import pytest

from xero.pipeline import pipelines
from xero import xero_service
from tasks import tasks_service

TIME_FRAME = [
    # ("auto", None),
    ("manual", "2010-01-01"),
]


@pytest.fixture(
    params=[i[1] for i in TIME_FRAME],
    ids=[i[0] for i in TIME_FRAME],
)
def start(request):
    return request.param


class TestXero:
    @pytest.mark.parametrize(
        "pipeline",
        pipelines.values(),
        ids=pipelines.keys(),
    )
    def test_service(self, pipeline, start):
        res = xero_service.pipeline_service(pipeline, start)
        print(res)
        assert res["output_rows"] >= 0


class TestTasks:
    def test_service(self, start):
        res = tasks_service.create_tasks_service(
            {
                "start": start,
            }
        )
        print(res)
        assert res["tasks"] > 0
