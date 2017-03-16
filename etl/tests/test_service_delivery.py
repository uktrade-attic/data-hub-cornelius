# -*- coding: utf-8 -*-

import json
import os.path

from .. import leeloo

__here__ = os.path.dirname(__file__)


def get_fixture(name):
    path = os.path.join(__here__, "fixtures", "{}.json".format(name))
    with open(path) as f:
        return json.load(f)


def test_cdms_to_leeloo():
    data = get_fixture('service_delivery_cdms')
    result = leeloo.service_delivery(data)
    expected = get_fixture('service_delivery_leeloo')
    assert result == expected
