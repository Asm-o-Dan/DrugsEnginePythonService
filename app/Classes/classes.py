from dataclasses import dataclass, field
from typing import List
import uuid

@dataclass
class Country:
    name: str
    code: str
    id: uuid.UUID
    drugs: List['Drug'] = field(default_factory=list)

@dataclass
class DrugItem:
    # Добавьте необходимые поля для DrugItem
    pass

@dataclass
class Drug:
    name: str
    manufacturer: str
    country_code_id: str
    country: None
    drug_items: List[DrugItem] = field(default_factory=list)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def from_json(cls, data: dict):
        return cls(
            name=data.get('Name'),
            manufacturer=data.get('Manufacturer'),
            country_code_id=data.get('CountryCodeId'),
            country=None,
            drug_items= []
        )

    def to_json(self):
        return {
            'Name': self.name,
            'Manufacturer': self.manufacturer,
            'CountryCodeId': self.country_code_id,
            'Country': {
                'Name': self.country.name,
                'Code': self.country.code,
                'Drugs': self.country.drugs,
                'Id': self.country.id
            },
            'DrugItems': [item.__dict__ for item in self.drug_items],
            'Id': self.id
        }