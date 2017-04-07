# -*- coding: utf-8 -*-

import logging

import requests
from pyquery import PyQuery


logger = logging.getLogger('cmds_api')

default_headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}


def dictify(*args, **kwargs):
    result = {}
    for item in args:
        if item:
            result.update(item)
    result.update(kwargs)
    return result


def login(url, username, password, user_agent=None):
    """
    This goes through the following steps:

    1. get login page
    2. submit the form with username and password
    3. the result is a form with a security token issued by the STS and the
       url of the next STS to validate the token
    4. submit the form of step 3. without making any changes
    5. repeat step 3. and 4 one more time to get the valid authentication
       cookie

    For more details, check:
    https://msdn.microsoft.com/en-us/library/aa480563.aspx
    """
    session = requests.session()
    if user_agent:
        session.headers.update({'User-Agent': user_agent})
    # 1. get login page
    # url = '{}/?whr={}'.format(CDMS_BASE_URL, CDMS_ADFS_URL)
    resp = session.get(url)
    assert resp.ok

    html_parser = PyQuery(resp.text)
    username_field_name = html_parser('input[name*="Username"]').attr('name')
    password_field_name = html_parser('input[name*="Password"]').attr('name')

    # 2. submit the login form with username and password
    resp = submit_form(
        session, resp.content,
        url=resp.url,
        params={
            username_field_name: username,
            password_field_name: password})

    # 3. and 4. re-submit the resulting form containing the security token
    # so that the next STS can validate it
    resp = submit_form(session, resp.content)

    # 5. re-submit the form again to validate the token and get as result
    # the authenticated cookie
    submit_form(session, resp.content)
    return session


def submit_form(session, source, url=None, params={}):
    """
    It submits the form contained in the `source` param optionally
    overriding form `params` and form `url`.

    This is needed as UKTI has a few STSes and the token has to be
    validated by all of them.  For more details, check:
    https://msdn.microsoft.com/en-us/library/aa480563.aspx
    """
    html_parser = PyQuery(source)
    form_action = html_parser('form').attr('action')

    # get all inputs in the source + optional params passed in
    data = {
        field.get('name'): field.get('value')
        for field in html_parser('input')
    }
    data.update(params)

    url = url or form_action
    resp = session.post(url, data)

    assert resp.ok

    html_parser = PyQuery(resp.content)
    assert form_action != html_parser('form').attr('action')

    return resp
