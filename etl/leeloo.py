# -*- coding: utf-8 -*-

from . import utils


service_delivery_attributes = {
    "date": "optevia_OrderDate",
}

service_delivery_relationships = {
    "company": "optevia_Organisation",
    "contact": "optevia_Contact",
    "country_of_interest": "optevia_LeadCountry",
    "dit_advisor": "optevia_Advisor",
    "dit_team": "optevia_ServiceProvider",
    "sector": "optevia_Sector",
    "service": "optevia_Service",
    "service_offer": "optevia_ServiceOffer",
    "status": "optevia_ServiceDeliveryStatus",
    "uk_region": "optevia_UKRegion",
}


entity_map = {
    "optevia_Organisation": "Company",
    "optevia_Contact": "Contact",
    "optevia_LeadCountry": "Country",
    "optevia_Advisor": "Advisor",
    "optevia_ServiceProvider": "Team",
    "optevia_Sector": "Sector",
    "optevia_Service": "Service",
    "optevia_ServiceOffer": "Service_Offer",
    "optevia_ServiceDeliveryStatus": "Status",
    "optevia_UKRegion": "UKRegion",
}


def make_relationship(data, name):
    _id = data[name]['Id']
    return {
        'data': {
            "id": _id,
            "type": entity_map[name]}}


date_keys = frozenset(['modified_on', 'created_on', 'date'])


def service_delivery(data):
    result = {}
    attributes = {}
    for leeloo_name, cdms_name in service_delivery_attributes.items():
        attributes[leeloo_name] = data[cdms_name]
    result['attributes'] = attributes
    relationships = {}
    for leeloo_name, cdms_name in service_delivery_relationships.items():
        relationships[leeloo_name] = make_relationship(data, cdms_name)
    result['relationships'] = relationships
    return result
